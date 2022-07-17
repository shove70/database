module database.pool;

import core.thread;
import std.concurrency;
import std.datetime;
import std.algorithm : any, remove;
import std.exception : enforce, collectException;

final class ConnectionProvider(Connection, alias flags) {
	private alias ConnectionPool = typeof(this),
	Flags = typeof(cast()flags);

	static ConnectionPool getInstance(string host, string user, string password, string database,
		ushort port = flags ? 3306 : 5432, uint maxConnections = 10, uint initialConnections = 3,
		uint incrementalConnections = 3, uint waitSeconds = 5, Flags caps = flags)
	in (initialConnections && incrementalConnections) {
		if (_instance is null) {
			synchronized (ConnectionPool.classinfo) {
				if (_instance is null) {
					_instance = new ConnectionPool(host, user, password, database, port,
						maxConnections, initialConnections, incrementalConnections, waitSeconds, caps);
				}
			}
		}

		return _instance;
	}

	private this(string host, string user, string password, string database, ushort port,
		uint maxConnections, uint initialConnections, uint incrementalConnections, uint waitSeconds,
		Flags caps) shared {
		_pool = cast(shared)spawn(new shared Pool(host, user, password, database, port,
				maxConnections, initialConnections, incrementalConnections, waitSeconds.seconds, caps));
		_waitSeconds = waitSeconds;
		while (!_instantiated)
			Thread.sleep(0.msecs);
	}

	~this() shared {
		(cast()_pool).send(new shared Terminate(cast(shared)thisTid));

	L_receive:
		try
			receive((shared Terminate _) {});
		catch (OwnerTerminated e) {
			if (e.tid != thisTid)
				goto L_receive;
		}

		_instantiated = false;
	}

	Connection getConnection() shared {
		(cast()_pool).send(new shared RequestConnection(cast(shared Tid)thisTid));
		Connection conn;

	L_receive:
		try {
			receiveTimeout(
				_waitSeconds.seconds,
				(shared Connection c) { conn = cast()c; },
				(immutable ConnectionBusy _) { conn = null; }
			);
		} catch (OwnerTerminated e) {
			if (e.tid != thisTid)
				goto L_receive;
		}

		return conn;
	}

	void releaseConnection(ref Connection conn) shared {
		enforce(conn.pooled, "This connection is not a managed connection in the pool.");
		enforce(!conn.inTransaction, "This connection also has uncommitted or unrollbacked transaction.");

		(cast()_pool).send(cast(shared)conn);
		conn = null;
	}

private:
	__gshared ConnectionPool _instance;

	Tid _pool;
	uint _waitSeconds;

	shared static bool _instantiated;

	class Pool {
		this(string host, string user, string password, string database, ushort port,
			uint maxConnections, uint initialConnections, uint incrementalConnections, Duration waitTime,
			Flags caps) shared {
			_host = host;
			_user = user;
			_password = password;
			_database = database;
			_port = port;
			_maxConnections = maxConnections;
			_initialConnections = initialConnections;
			_incrementalConnections = incrementalConnections;
			_waitTime = waitTime;
			static if (flags)
				_flags = caps;

			createConnections(initialConnections);
			_lastShrinkTime = cast(DateTime)Clock.currTime;
			_instantiated = true;
		}

		void opCall() shared {
			bool loop = true;

			while (loop) {
				try {
					receive(
						(shared RequestConnection req) { getConnection(req); },
						(shared Connection conn) { releaseConnection(conn); },
						(shared Terminate t) {
						foreach (conn; _pool)
							(cast()conn).close();

						(cast()t.tid).send(t);
						loop = false;
					}
					);
				} catch (OwnerTerminated) {
				}

				// Shrink the pool.
				auto now = cast(DateTime)Clock.currTime;
				if (now - _lastShrinkTime > 60.seconds && _pool.length > _initialConnections) {
					foreach (ref conn; cast(Connection[])_pool) {
						if (conn is null || conn.busy || now - conn.releaseTime <= 120.seconds)
							continue;

						collectException({ conn.close(); }());
						conn = null;
					}

					if (_pool.any!((a) => (a is null)))
						_pool = _pool.remove!((a) => a is null);

					_lastShrinkTime = now;
				}
			}
		}

	private:
		Connection createConnection() shared {
			try {
				static if (flags)
					auto conn = new Connection(_host, _user, _password, _database, _port, _flags);
				else
					auto conn = new Connection(_host, _user, _password, _database, _port);
				conn.pooled = true;
				return conn;
			} catch (Exception)
				return null;
		}

		void createConnections(uint num) shared {
			foreach (i; 0 .. num) {
				if (_maxConnections > 0 && _pool.length >= _maxConnections)
					break;

				if (auto conn = createConnection())
					_pool ~= cast(shared)conn;
			}
		}

		void getConnection(shared RequestConnection req) shared {
			immutable start = Clock.currTime;

			while (true) {
				if (auto conn = getFreeConnection()) {
					(cast()req.tid).send(cast(shared)conn);
					return;
				}

				if (Clock.currTime - start >= _waitTime)
					break;

				Thread.sleep(100.msecs);
			}

			(cast()req.tid).send(new immutable ConnectionBusy);
		}

		Connection getFreeConnection() shared {
			Connection conn = findFreeConnection();

			if (conn is null) {
				createConnections(_incrementalConnections);
				conn = findFreeConnection();
			}

			return conn;
		}

		Connection findFreeConnection() shared {
			Connection result;

			foreach (conn; _pool) {
				if (conn is null || conn.busy)
					continue;

				if (!testConnection(cast()conn))
					continue;

				conn.busy = true;
				result = cast()conn;
				break;
			}

			if (_pool.any!((a) => a is null)) {
				_pool = _pool.remove!((a) => a is null);
			}

			return result;
		}

		bool testConnection(Connection conn) shared {
			try {
				conn.ping();
				return true;
			} catch (Exception) {
				collectException({ conn.close(); }());
				conn = null;

				return false;
			}
		}

		void releaseConnection(shared Connection c) shared {
			if (auto conn = cast()c) {
				conn.busy = false;
				conn.releaseTime = cast(DateTime)Clock.currTime;
			}
		}

		Connection[] _pool;

		string _host;
		string _user;
		string _password;
		string _database;
		ushort _port;
		uint _maxConnections;
		uint _initialConnections;
		uint _incrementalConnections;
		Duration _waitTime;
		static if (flags)
			Flags _flags;

		DateTime _lastShrinkTime;
	}
}

private:

template TID() {
	Tid tid;

	this(shared Tid tid) shared {
		this.tid = tid;
	}
}

shared class RequestConnection {
	mixin TID;
}

immutable class ConnectionBusy {
}

shared class Terminate {
	mixin TID;
}

/+
unittest {
	import core.thread;
	import std.stdio;

	auto pool = ConnectionPool.getInstance("127.0.0.1", "root", "111111", "test", 3306);
	int i;
	while (i++ < 20) {
		Thread.sleep(100.msecs);
		Connection conn = pool.getConnection();
		if (conn) {
			writeln(conn.connected());
			pool.releaseConnection(conn);
		}
	}
	pool.destroy();
}
+/

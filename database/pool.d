module database.pool;

import core.thread;
import std.concurrency;
import std.datetime;
import std.exception : enforce;

final class ConnectionProvider(Connection, alias flags) {
	private alias ConnectionPool = typeof(this),
	Flags = typeof(cast()flags);

	static getInstance(string host, string user, string password, string database,
		ushort port = flags ? 3306 : 5432, uint maxConnections = 10, uint initialConnections = 3,
		uint incrementalConnections = 3, Duration waitTime = 5.seconds, Flags caps = flags)
	in (initialConnections && incrementalConnections) {
		if (!_instance) {
			synchronized (ConnectionPool.classinfo) {
				if (!_instance)
					_instance = new ConnectionPool(host, user, password, database, port,
						maxConnections, initialConnections, incrementalConnections, waitTime, caps);
			}
		}

		return _instance;
	}

	private this(string host, string user, string password, string database, ushort port,
		uint maxConnections, uint initialConnections, uint incrementalConnections, Duration waitTime,
		Flags caps) shared {
		_pool = cast(shared)spawn(Pool(host, user, password, database, port,
				maxConnections, initialConnections, incrementalConnections, waitTime, caps));
		while (!_instantiated)
			Thread.sleep(0.msecs);
	}

	~this() shared {
		(cast()_pool).send(Terminate(thisTid));

	L_receive:
		try
			receive((Terminate _) {});
		catch (OwnerTerminated e) {
			if (e.tid != thisTid)
				goto L_receive;
		}

		_instantiated = false;
	}

	Connection getConnection(Duration waitTime = 5.seconds) shared {
		(cast()_pool).send(RequestConnection(thisTid));
		Connection conn;

	L_receive:
		try {
			receiveTimeout(
				waitTime,
				(shared Connection c) { conn = cast()c; },
				(immutable ConnBusy _) { conn = null; }
			);
		} catch (OwnerTerminated e) {
			if (e.tid != thisTid)
				goto L_receive;
		}

		return conn;
	}

	void releaseConnection(ref Connection conn) {
		enforce(conn.pooled, "This connection is not a managed connection in the pool.");
		enforce(!conn.inTransaction, "This connection also has uncommitted or unrollbacked transaction.");

		_pool.send(cast(shared)conn);
		conn = null;
	}

private:
	__gshared ConnectionPool _instance;

	Tid _pool;

	shared static bool _instantiated;

	static struct Pool {
		this(string host, string user, string password, string database, ushort port,
			uint maxConnections, uint initialConnections, uint incrementalConnections, Duration waitTime,
			Flags caps) {
			_host = host;
			_user = user;
			_pwd = password;
			_db = database;
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

		void opCall() {
			bool loop = true;

			while (loop) {
				try {
					receive(
						(RequestConnection req) { getConnection(req); },
						(shared Connection conn) { releaseConnection(conn); },
						(Terminate t) {
						foreach (conn; _pool)
							(cast()conn).close();

						t.tid.send(t);
						loop = false;
					}
					);
				} catch (OwnerTerminated) {
				}

				// Shrink the pool.
				auto now = cast(DateTime)Clock.currTime;
				if (now - _lastShrinkTime > 60.seconds && _pool.length > _initialConnections) {
					typeof(_pool) tmp;
					foreach (ref conn; _pool) {
						if (conn && !(cast()conn).busy && now - conn.releaseTime > 120.seconds) {
							try
								(cast()conn).close();
							catch (Exception) {
							}
							conn = null;
						}
						if (conn)
							tmp ~= conn;
					}
					_pool = tmp;

					_lastShrinkTime = now;
				}
			}
		}

		Connection createConnection() {
			try {
				static if (flags)
					auto conn = new Connection(_host, _user, _pwd, _db, _port, _flags);
				else
					auto conn = new Connection(_host, _user, _pwd, _db, _port);
				conn.pooled = true;
				return conn;
			} catch (Exception)
				return null;
		}

		void createConnections(uint n) {
			while (n--) {
				if (_maxConnections > 0 && _pool.length >= _maxConnections)
					break;

				if (auto conn = createConnection())
					_pool ~= cast(shared)conn;
			}
		}

		void getConnection(RequestConnection req) {
			immutable start = Clock.currTime;

			while (true) {
				if (auto conn = getFreeConnection()) {
					req.tid.send(cast(shared)conn);
					return;
				}

				if (Clock.currTime - start >= _waitTime)
					break;

				Thread.sleep(100.msecs);
			}

			req.tid.send(ConnectionBusy);
		}

		Connection getFreeConnection() {
			Connection conn = findFreeConnection();

			if (conn is null) {
				createConnections(_incrementalConnections);
				conn = findFreeConnection();
			}

			return conn;
		}

		Connection findFreeConnection() {
			Connection result;

			foreach (conn; cast(Connection[])_pool) {
				if (conn is null || conn.busy)
					continue;

				if (testConnection(conn)) {
					conn.busy = true;
					result = conn;
					break;
				}
			}

			typeof(_pool) tmp;
			foreach (conn; _pool) {
				if (conn)
					tmp ~= conn;
			}
			_pool = tmp;

			return result;
		}

		bool testConnection(Connection conn) nothrow {
			try {
				conn.ping();
				return true;
			} catch (Exception) {
				conn.close();
				conn = null;

				return false;
			}
		}

		void releaseConnection(shared Connection conn) {
			(cast()conn).busy = false;
			conn.releaseTime = cast(DateTime)Clock.currTime;
		}

		shared(Connection)[] _pool;

		string _host;
		string _user;
		string _pwd;
		string _db;
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

struct RequestConnection {
	Tid tid;
}

immutable ConnectionBusy = ConnBusy();

struct ConnBusy {
}

struct Terminate {
	Tid tid;
}

/+
unittest {
	import core.thread;
	import std.stdio;

	auto pool = ConnectionPool.getInstance("127.0.0.1", "root", "111111", "test", 3306);

	foreach (i; 0 .. 20) {
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

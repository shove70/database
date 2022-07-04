module database.postgresql.pool;

import core.time;
import core.thread;
import std.stdio;
import std.array;
import std.concurrency;
import std.datetime;
import std.algorithm.searching : any;
import std.algorithm.mutation : remove;
import std.exception : enforce, collectException;

import database.postgresql.connection;

alias ConnectionPool = shared ConnectionProvider;

final class ConnectionProvider
{
    static ConnectionPool getInstance(string host, string user, string password, string database, ushort port = 5432,
        uint maxConnections = 10, uint initialConnections = 3, uint incrementalConnections = 3, uint waitSeconds = 5,
        ConnectionOptions options = ConnectionOptions.Default)
    {
        assert(initialConnections > 0 && incrementalConnections > 0);

        if (_instance is null)
        {
            synchronized(ConnectionProvider.classinfo)
            {
                if (_instance is null)
                {
                    _instance = new ConnectionPool(host, user, password, database, port,
                        maxConnections, initialConnections, incrementalConnections, waitSeconds, options);
                }
            }
        }

        return _instance;
    }

    private this(string host, string user, string password, string database, ushort port,
        uint maxConnections, uint initialConnections, uint incrementalConnections, uint waitSeconds,
        ConnectionOptions options) shared
    {
        _pool = cast(shared Tid)spawn(new shared Pool(host, user, password, database, port,
            maxConnections, initialConnections, incrementalConnections, waitSeconds.seconds, options));
        _waitSeconds = waitSeconds;
        while (!__instantiated) Thread.sleep(0.msecs);
    }

    ~this() shared
    {
        (cast(Tid)_pool).send(new shared Terminate(cast(shared Tid)thisTid));

        L_receive: try
        {
            receive(
                (shared Terminate _t)
                {
                    return;
                }
            );
        }
        catch (OwnerTerminated e)
        {
            if (e.tid != thisTid) goto L_receive;
        }

        __instantiated = true;
    }

    Connection getConnection() shared
    {
        (cast(Tid)_pool).send(new shared RequestConnection(cast(shared Tid)thisTid));
        Connection conn;

        L_receive: try
        {
            receiveTimeout(
                _waitSeconds.seconds,
                (shared ConnenctionHolder holder)
                {
                    conn = cast(Connection)holder.conn;
                },
                (immutable ConnectionBusy _m)
                {
                    conn = null;
                }
            );
        }
        catch (OwnerTerminated e)
        {
            if (e.tid != thisTid) goto L_receive;
        }

        return conn;
    }

    void releaseConnection(ref Connection conn) shared
    {
        enforce(conn.pooled, "This connection is not a managed connection in the pool.");
        enforce(!conn.inTransaction, "This connection also has uncommitted or unrollbacked transaction.");

        (cast(Tid)_pool).send(new shared ConnenctionHolder(cast(shared Connection)conn));
        conn = null;
    }

private:

    __gshared ConnectionPool _instance = null;

    Tid _pool;
    int _waitSeconds;
}

private:

shared bool __instantiated;

class Pool
{
    this(string host, string user, string password, string database, ushort port,
        uint maxConnections, uint initialConnections, uint incrementalConnections, Duration waitTime,
        ConnectionOptions options) shared
    {
        _host = host;
        _user = user;
        _password = password;
        _database = database;
        _port = port;
        _maxConnections = maxConnections;
        _initialConnections = initialConnections;
        _incrementalConnections = incrementalConnections;
        _waitTime = waitTime;
        _options = options;

        createConnections(initialConnections);
        _lastShrinkTime = cast(DateTime)Clock.currTime;
        __instantiated = true;
    }

    void opCall() shared
    {
        auto loop = true;

        while (loop)
        {
            try
            {
                receive(
                    (shared RequestConnection req)
                    {
                        getConnection(req);
                    },
                    (shared ConnenctionHolder holder)
                    {
                        releaseConnection(holder);
                    },
                    (shared Terminate t)
                    {
                        foreach (conn; _pool)
                        {
                            (cast(Connection)conn).close();
                        }

                        (cast(Tid)t.tid).send(t);
                        loop = false;
                    }
                );
            }
            catch (OwnerTerminated e) { }

            // Shrink the pool.
            DateTime now = cast(DateTime)Clock.currTime;
            if (((now - _lastShrinkTime) > 60.seconds) && (_pool.length > _initialConnections))
            {
                foreach (ref conn; cast(Connection[])_pool)
                {
                    if ((conn is null) || conn.busy || ((now - conn.releaseTime) <= 120.seconds))
                    {
                        continue;
                    }

                    collectException({ conn.close(); }());
                    conn = null;
                }

                if (_pool.any!((a) => (a is null)))
                {
                    _pool = _pool.remove!((a) => (a is null));
                }

                _lastShrinkTime = now;
            }
        }
    }

private:

    Connection createConnection() shared
    {
        try
        {
            Connection conn = new Connection(_host, _user, _password, _database, _port, _options);
            conn.pooled = true;
            return conn;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    void createConnections(uint num) shared
    {
        for (int i; i < num; i++)
        {
            if ((_maxConnections > 0) && (_pool.length >= _maxConnections))
            {
                break;
            }

            Connection conn = createConnection();

            if (conn !is null)
            {
                _pool ~= cast(shared Connection)conn;
            }
        }
    }

    void getConnection(shared RequestConnection req) shared
    {
        immutable start = Clock.currTime();

        while (true)
        {
            Connection conn = getFreeConnection();

            if (conn !is null)
            {
                (cast(Tid)req.tid).send(new shared ConnenctionHolder(cast(shared Connection)conn));
                return;
            }

            if ((Clock.currTime() - start) >= _waitTime)
            {
                break;
            }
 
            Thread.sleep(100.msecs);
        }

        (cast(Tid)req.tid).send(new immutable ConnectionBusy);
    }

    Connection getFreeConnection() shared
    {
        Connection conn = findFreeConnection();

        if (conn is null)
        {
            createConnections(_incrementalConnections);
            conn = findFreeConnection();
        }     

        return conn;
    }

    Connection findFreeConnection() shared
    {
        Connection result;

        for (size_t i = 0; i < _pool.length; i++)
        {
            Connection conn = cast(Connection)_pool[i];

            if ((conn is null) || conn.busy)
            {
                continue;
            }

            if (!testConnection(conn))
            {
                continue;
            }

            conn.busy = true;
            result = conn;
            break;
        }

        if (_pool.any!((a) => (a is null)))
        {
            _pool = _pool.remove!((a) => (a is null));
        }

        return result;
    }

    bool testConnection(Connection conn) shared
    {
        try
        {
            conn.ping();
            return true;
        }
        catch (Exception e)
        {
            collectException({ conn.close(); }());
            conn = null;

            return false;
        }
    }

    void releaseConnection(shared ConnenctionHolder holder) shared
    {
        if (holder.conn !is null)
        {
            Connection conn = cast(Connection)holder.conn;
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
    ConnectionOptions _options;

    DateTime _lastShrinkTime;
}

shared class RequestConnection
{
    Tid tid;

    this(shared Tid tid) shared
    {
        this.tid = tid;
    }
}

shared class ConnenctionHolder
{
    Connection conn;

    this(shared Connection conn) shared
    {
        this.conn = conn;
    }
}

immutable class ConnectionBusy
{
}

shared class Terminate
{
    Tid tid;

    this(shared Tid tid) shared
    {
        this.tid = tid;
    }
}

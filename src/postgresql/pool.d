module postgresql.pool;

version(POSTGRESQL):

import core.time;
import core.thread;
import std.stdio;
import std.array;
import std.concurrency;
import std.datetime;

import postgresql.connection;

alias ConnectionPool = shared ConnectionProvider;

final class ConnectionProvider
{
    static ConnectionPool getInstance(string host, string user, string password, string database, ushort port = 5432,
        uint maxConnections = 10, uint initialConnections = 3, uint incrementalConnections = 3, uint waitSeconds = 5, ConnectionOptions options = ConnectionOptions.Default)
    {
        assert(initialConnections > 0 && incrementalConnections > 0);

        if (_instance is null)
        {
            synchronized(ConnectionProvider.classinfo)
            {
                if (_instance is null)
                {
                    _instance = new ConnectionPool(host, user, password, database, port, maxConnections, initialConnections, incrementalConnections, waitSeconds, options);
                }
            }
        }

        return _instance;
    }

    private this(string host, string user, string password, string database, ushort port, uint maxConnections, uint initialConnections, uint incrementalConnections, uint waitSeconds, ConnectionOptions options) shared
    {
        _pool = cast(shared Tid)spawn(new shared Pool(host, user, password, database, port, maxConnections, initialConnections, incrementalConnections, waitSeconds.dur!"seconds", options));
        _waitSeconds = waitSeconds;
    }

    ~this() shared
    {
        (cast(Tid)_pool).send(new shared Terminate(cast(shared Tid)thisTid));

        receive(
            (shared Terminate _t)
            {
                return;
            }
        );
    }

    Connection getConnection() shared
    {
        (cast(Tid)_pool).send(new shared RequestConnection(cast(shared Tid)thisTid));
        Connection ret;

        receiveTimeout(
            _waitSeconds.dur!"seconds",
            (shared ConnenctionHolder holder)
            {
                ret = cast(Connection)holder.conn;
            },
            (immutable ConnectionBusy _m)
            {
                ret = null;
            }
        );

        return ret;
    }

    void releaseConnection(ref Connection conn) shared
    {
        (cast(Tid)_pool).send(new shared ConnenctionHolder(cast(shared Connection)conn));
        conn = null;
    }

private:

    __gshared ConnectionPool _instance = null;

    Tid _pool;
    int _waitSeconds;
}

private:

class Pool
{
    this(string host, string user, string password, string database, ushort port, uint maxConnections, uint initialConnections, uint incrementalConnections, Duration waitTime, ConnectionOptions options) shared
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

                        loop = false;
                        (cast(Tid)t.tid).send(t);
                    }
                );
            }
            catch (OwnerTerminated e)
            {
                loop = false;
            }
        }
    }

private:

    void createConnections(uint num) shared
    {
        for (int i; i < num; i++)
        {
            if ((_maxConnections > 0) && (_pool.length >= _maxConnections))
            {
                break;
            }

            _pool ~= cast(shared Connection)new Connection(
                    this._host,
                    this._user,
                    this._password,
                    this._database,
                    this._port,
                    this._options);
        }
    }

    void getConnection(shared RequestConnection req) shared
    {
        auto start = Clock.currTime();

        do
        {
            Connection conn = getFreeConnection();

            if (conn is null)
            {
                Thread.sleep(100.msecs);
                conn = getFreeConnection();
            }

            if (conn !is null)
            {
                if (!testConnection(conn))
                {
                    conn = new Connection(_host, _user, _password, _database, _port, _options);
                }

                conn.busy = true;
                (cast(Tid)req.tid).send(new shared ConnenctionHolder(cast(shared Connection)conn));

                return;
            }
        } while ((Clock.currTime() - start) < _waitTime);

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
        for (size_t i = 0; i < _pool.length; i++)
        {
            Connection conn = cast(Connection)_pool[i];

            if (!conn.busy)
            {
                if (!testConnection(conn))
                {
                    conn = new Connection(
                        this._host,
                        this._user,
                        this._password,
                        this._database,
                        this._port,
                        this._options);
                }

                conn.busy = true;

                return conn;
            }
        }

        return null;
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
            return false;
        }
    }

    void releaseConnection(shared ConnenctionHolder holder) shared
    {
        if (holder.conn !is null)
        {
            Connection conn = cast(Connection)holder.conn;
            conn.busy = false;
        }
    }

private:

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

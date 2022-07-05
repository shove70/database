module database.postgresql.connection;

import std.algorithm;
import std.array;
import std.conv : to;
import std.regex : ctRegex, matchFirst;
import std.string;
import std.traits;
import std.uni : sicmp;
import std.utf : decode, UseReplacementDchar;
import std.format;
import std.datetime;

import database.postgresql.exception;
import database.postgresql.packet;
import database.postgresql.protocol;
import database.postgresql.type;
import database.postgresql.row;
import database.postgresql.appender;
import database.util;

alias Socket = DBSocket!PgSQLConnectionException;

struct ConnectionStatus
{
    bool ready;
    TransactionStatus transaction = TransactionStatus.Idle;

    ulong affected;
    ulong lastInsertId;
}

struct ConnectionNotice
{
    enum Severity : ubyte
    {
        ERROR = 1,
        FATAL,
        PANIC,
        WARNING,
        NOTICE,
        DEBUG,
        INFO,
        LOG,
    }

    Severity severity;
    uint position;
    const(char)[] message;
    const(char)[] code;
    const(char)[] hint;
    const(char)[] detail;
    const(char)[] where;
    const(char)[] schema;
    const(char)[] table;
    const(char)[] column;
    const(char)[] type;
    const(char)[] constraint;

    string toString() const
    {
        auto writer = appender!string;
        toString(writer);
        return writer.data;
    }

    void toString(W)(ref W writer) const
    {
        writer.formattedWrite("%s(%s) %s", severity, code, message);
    }
}

private struct ConnectionSettings
{
    this(const(char)[] connectionString)
    {
        parse(connectionString);
    }

    void parse(const(char)[] connectionString)
    {
        auto remaining = connectionString;

        auto indexValue = remaining.indexOf("=");
        while (!remaining.empty)
        {
            auto indexValueEnd = remaining.indexOf(";", indexValue);
            if (indexValueEnd <= 0)
                indexValueEnd = remaining.length;

            auto name = strip(remaining[0..indexValue]);
            auto value = strip(remaining[indexValue+1..indexValueEnd]);

            switch (name)
            {
                case "host":
                    host = value;
                    break;
                case "user":
                    user = value;
                    break;
                case "pwd":
                    pwd = value;
                    break;
                case "db":
                    db = value;
                    break;
                case "port":
                    port = to!ushort(value);
                    break;
                default:
                    throw new PgSQLException(format("Bad connection string: %s", connectionString));
            }

            if (indexValueEnd == remaining.length)
                return;

            remaining = remaining[indexValueEnd+1..$];
            indexValue = remaining.indexOf("=");
        }

        throw new PgSQLException(format("Bad connection string: %s", connectionString));
    }

    ConnectionOptions options = ConnectionOptions.Default;

    const(char)[] host;
    const(char)[] user;
    const(char)[] pwd;
    const(char)[] db;
    ushort port = 3306;
}

private struct ServerInfo
{
    const(char)[] versionString;
    const(char)[] encoding;
    const(char)[] application;
    const(char)[] timeZone;

    uint processId;
    uint cancellationKey;
}

enum ConnectionOptions
{
    Default        = 0
}

class Connection
{
    this(string connectionString, ConnectionOptions options = ConnectionOptions.Default)
    {
        settings_ = ConnectionSettings(connectionString);
        settings_.options = options;
        connect();
    }

    this(const(char)[] host, const(char)[] user, const(char)[] pwd, const(char)[] db, ushort port = 5432)
    {
        this(host, user, pwd, db, port, ConnectionOptions.Default);
    }

    this(const(char)[] host, const(char)[] user, const(char)[] pwd, const(char)[] db, ushort port = 5432, ConnectionOptions options = ConnectionOptions.Default)
    {
        settings_.host = host;
        settings_.user = user;
        settings_.pwd = pwd;
        settings_.db = db;
        settings_.port = port;
        settings_.options = options;

        connect();
    }

    void ping()
    {
    }

    const(char)[] schema() const
    {
        return schema_;
    }

    const(ConnectionNotice)[] notices() const
    {
        return notices_;
    }

    void execute(Args...)(const(char)[] sql, Args args)
    {
        query(sql, args);
    }

    void set(T)(const(char)[] variable, T value)
    {
        //query("set session ?=?", PgSQLFragment(variable), value);
    }

    const(char)[] get(const(char)[] variable)
    {
        const(char)[] result;
        query("show session variables like ?", variable, (PgSQLRow row) {
            result = row[1].peek!(const(char)[]).dup;
        });

        return result;
    }

    void startTransaction()
    {
        if (inTransaction)
        {
            throw new PgSQLErrorException("PgSQL does not support nested transactions - commit or rollback before starting a new transaction");
        }

        query("start transaction");

        assert(inTransaction);
    }

    void commit()
    {
        if (!inTransaction)
        {
            throw new PgSQLErrorException("No active transaction");
        }

        query("commit");

        assert(!inTransaction);
    }

    void rollback()
    {
        if (connected)
        {
            if (status_.transaction != TransactionStatus.Inside)
            {
                throw new PgSQLErrorException("No active transaction");
            }

            query("rollback");

            assert(!inTransaction);
        }
    }

    @property bool inTransaction() const
    {
        return connected && (status_.transaction == TransactionStatus.Inside);
    }

    @property ulong lastInsertId() const
    {
        return status_.lastInsertId;
    }

    @property ulong affected() const
    {
        return status_.affected;
    }

    @property bool connected() const
    {
        return socket_.connected;
    }

    void close()
    {
        socket_.close();
    }

    void reuse()
    {
        ensureConnected();

        if (inTransaction)
        {
            rollback;
        }
    }

package:

    @property bool busy()
    {
        return busy_;
    }

    @property void busy(bool value)
    {
        busy_ = value;
    }

    @property bool pooled()
    {
        return pooled_;
    }

    @property void pooled(bool value)
    {
        pooled_ = value;
    }

    @property DateTime releaseTime()
    {
        return releaseTime_;
    }

    @property void releaseTime(DateTime value)
    {
        releaseTime_ = value;
    }

private:

    void close_()
    {
        close();
    }

    void query(Args...)(const(char)[] sql, Args args)
    {
        //scope(failure) close_();

        static if (args.length == 0)
        {
            enum shouldDiscard = true;
        }
        else
        {
            enum shouldDiscard = !isCallable!(args[args.length - 1]);
        }

        enum argCount = shouldDiscard ? args.length : (args.length - 1);

        static if (argCount)
        {
            auto querySQL = prepareSQL(sql, args[0..argCount]);
        }
        else
        {
            auto querySQL = sql;
        }

        send(querySQL);

        auto answer = retrieve();
        if (isStatus(answer))
        {
            eatStatuses(answer);
        }
        else
        {
            static if (!shouldDiscard)
            {
                resultSetText(answer, args[args.length - 1]);
            }
            else
            {
                discardAll(answer);
            }
        }
    }

    void connect()
    {
        socket_.connect(settings_.host, settings_.port);

        auto startup = OutputPacket(&out_);
        startup.put!uint(0x00030000);
        startup.putz("user");
        startup.putz(settings_.user);
        if (!settings_.db.empty())
        {
            startup.putz("database");
            startup.putz(settings_.db);
        }
        startup.put!ubyte(0);
        startup.finalize();

        socket_.write(startup.get());

        if (eatAuth(retrieve()))
            eatAuth(retrieve());
        eatStatuses(retrieve());
    }

    void send(Args...)(Args args)
    {
        ensureConnected();

        auto cmd = OutputPacket(OutputMessageType.Query, &out_);
        foreach (ref arg; args)
            cmd.put!(const(char[]))(arg);
        cmd.put!ubyte(0);
        cmd.finalize();

        socket_.write(cmd.get());
    }

    void ensureConnected()
    {
        if (!socket_.connected)
        {
            connect();
        }
    }

    bool isStatus(InputPacket packet)
    {
        auto id = packet.type;

        switch (id) with (InputMessageType)
        {
            case ErrorResponse:
            case NoticeResponse:
            case ReadyForQuery:
            case NotificationResponse:
            case CommandComplete:
                return true;
            default:
                return false;
        }
    }

    InputPacket retrieve(ubyte control)
    {
        //scope(failure) close_();

        ubyte[4] header;
        socket_.read(header);

        auto len = native!uint(header.ptr) - 4;
        in_.length = len;
        socket_.read(in_);

        if (in_.length != len)
        {
            throw new PgSQLConnectionException("Wrong number of bytes read");
        }

        return InputPacket(control, &in_);
    }

    InputPacket retrieve()
    {
        //scope(failure) close_();

        ubyte[5] header;
        socket_.read(header);

        auto len = native!uint(header.ptr + 1) - 4;
        in_.length = len;
        socket_.read(in_);

        if (in_.length != len)
        {
            throw new PgSQLConnectionException("Wrong number of bytes read");
        }

        return InputPacket(header[0], &in_);
    }

    bool eatAuth(InputPacket packet)
    {
        //scope(failure) close_();

        auto type = cast(InputMessageType)packet.type;

        switch (type) with (InputMessageType)
        {
            case Authentication:
                auto auth = packet.eat!uint;
                auto reply = OutputPacket(OutputMessageType.PasswordMessage, &out_);

                switch (auth)
                {
                    case 0:
                        return false;
                    case 2:
                        goto default;
                    case 3:
                        reply.putz(settings_.pwd);
                        break;
                    case 5:
                        static char[32] MD5toHex(T...)(in T data)
                        {
                            import std.ascii : LetterCase;
                            import std.digest.md : md5Of, toHexString;
                            return md5Of(data).toHexString!(LetterCase.lower);
                        }

                        auto salt = packet.eat!(ubyte[])(4);
                        reply.put("md5");
                        reply.putz(MD5toHex(MD5toHex(settings_.pwd, settings_.user), salt));
                        break;
                    case 6: // SCM
                    case 7: // GSS
                    case 8:
                    case 9:
                    case 10: // SASL
                    case 11:
                    case 12:
                        goto default;
                    default:
                        throw new PgSQLProtocolException(format("Unsupported authentication method: %s", auth));
                }

                reply.finalize();
                socket_.write(reply.get());
                break;
            case NoticeResponse:
                eatNoticeResponse(packet);
                break;
            case ErrorResponse:
                eatNoticeResponse(packet);
                throwError(true);
                break;
            default:
                throw new PgSQLProtocolException(format("Unexpected message: %s", type));
        }

        return true;
    }

    void eatParameterStatus(InputPacket packet)
    {
        assert(packet.type == InputMessageType.ParameterStatus);
        auto name = packet.eatz();
        auto value = packet.eatz();

        switch (name)
        {
            case "server_version":
                server_.versionString = value.dup;
                break;
            case "server_encoding":
                server_.encoding = value.dup;
                break;
            case "application_name":
                server_.application = value.dup;
                break;
            case "TimeZone":
                server_.timeZone = value.dup;
                break;
            default:
                break;
        }
        assert(packet.empty());
    }

    void eatBackendKeyData(InputPacket packet)
    {
        assert(packet.type == InputMessageType.BackendKeyData);

        server_.processId = packet.eat!uint;
        server_.cancellationKey = packet.eat!uint;
    }

    void eatNoticeResponse(InputPacket packet)
    {
        assert(packet.type == InputMessageType.NoticeResponse || packet.type == InputMessageType.ErrorResponse);

        ConnectionNotice notice;
        auto field = packet.eat!ubyte;
        while (field)
        {
            auto value = packet.eatz();

            switch (field) with (NoticeMessageField)
            {
                case Severity:
                case SeverityLocal:
                    switch (hashOf(value)) with (ConnectionNotice.Severity)
                    {
                        case hashOf("ERROR"):
                            notice.severity = ERROR;
                            break;
                        case hashOf("FATAL"):
                            notice.severity = FATAL;
                            break;
                        case hashOf("PANIC"):
                            notice.severity = PANIC;
                            break;
                        case hashOf("WARNING"):
                            notice.severity = WARNING;
                            break;
                        case hashOf("DEBUG"):
                            notice.severity = DEBUG;
                            break;
                        case hashOf("INFO"):
                            notice.severity = INFO;
                            break;
                        case hashOf("LOG"):
                            notice.severity = LOG;
                            break;
                        default:
                            break;
                    }
                    break;
                case Code:
                    notice.code = value.idup;
                    break;
                case Message:
                    notice.message = value.idup;
                    break;
                case Detail:
                    notice.detail = value.idup;
                    break;
                case Hint:
                    notice.hint = value.idup;
                    break;
                case Position:
                    notice.position = value.to!uint;
                    break;
                case Where:
                    notice.where = value.idup;
                    break;
                case Schema:
                    notice.schema = value.idup;
                    break;
                case Table:
                    notice.table = value.idup;
                    break;
                case Column:
                    notice.column = value.idup;
                    break;
                case DataType:
                    notice.type = value.idup;
                    break;
                case Constraint:
                    notice.constraint = value.idup;
                    break;
                case File:
                case Line:
                case Routine:
                    break;
                default:
                    //writeln("  notice: ", cast(char)field, " ", value);
                    break;
            }
            field = packet.eat!ubyte;
        }

        notices_ ~= notice;
    }

    void eatCommandComplete(InputPacket packet)
    {
        assert(packet.type == InputMessageType.CommandComplete);

        auto tag = packet.eatz().splitter(' ');
        auto command = tag.front();
        tag.popFront();

        switch (hashOf(command))
        {
            case hashOf("INSERT"):
                status_.lastInsertId = tag.front().to!ulong;
                tag.popFront();
                status_.affected = tag.front().to!ulong;
                break;
            case hashOf("SELECT"):
            case hashOf("DELETE"):
            case hashOf("UPDATE"):
            case hashOf("MOVE"):
            case hashOf("FETCH"):
            case hashOf("COPY"):
                status_.lastInsertId = 0;
                status_.affected = tag.empty() ? 0 : tag.front().to!ulong;
                break;
            case hashOf("CREATE"):
            case hashOf("DROP"):
                status_.lastInsertId = 0;
                break;
            default:
                throw new PgSQLProtocolException(format("Unexpected command tag: %s", command));
        }
    }

    auto eatStatus(InputPacket packet)
    {
        auto type = cast(InputMessageType)packet.type();

        switch (type) with (InputMessageType)
        {
            case ParameterStatus:
                eatParameterStatus(packet);
                break;
            case BackendKeyData:
                eatBackendKeyData(packet);
                break;
            case ReadyForQuery:
                status_.transaction = cast(TransactionStatus)packet.eat!ubyte;
                status_.ready = true;
                break;
            case NoticeResponse:
                eatNoticeResponse(packet);
                break;
            case ErrorResponse:
                eatNoticeResponse(packet);
                throwError(true);
                break;
            case CommandComplete:
                eatCommandComplete(packet);
                break;
            default:
                throw new PgSQLProtocolException(format("Unexpected message: %s", type));
        }

        return type;
    }

    void throwError(bool force)
    {
        foreach (ref notice; notices_)
        {
            switch (notice.severity) with (ConnectionNotice.Severity)
            {
                case PANIC:
                case ERROR:
                case FATAL:
                    throw new PgSQLErrorException(cast(string)notice.message);
                default:
                    break;
            }
        }

        if (force)
            throw new PgSQLErrorException(cast(string)notices_.front().message);
    }

    void eatStatuses(InputPacket packet)
    {
        notices_.length = 0;

        auto status = eatStatus(packet);
        while (status != InputMessageType.ReadyForQuery)
            status = eatStatus(retrieve());
    }

    void skipColumnDef(ref InputPacket packet)
    {
        packet.skipz();
        packet.skip(18);
    }

    void columnDef(ref InputPacket packet, ref PgSQLColumn def)
    {
        auto name = packet.eatz();
        columns_ ~= name;
        def.name = columns_[$-name.length..$];

        packet.skip(6);

        def.type = cast(PgColumnTypes)packet.eat!uint;
        def.length = packet.eat!short;
        def.modifier = packet.eat!int;
        def.format = cast(FormatCode)packet.eat!short;
    }

    void columnDefs(size_t count, ref PgSQLColumn[] defs, ref InputPacket packet)
    {
        defs.length = count;
        foreach (i; 0..count)
            columnDef(packet, defs[i]);
    }

    bool callHandler(RowHandler)(RowHandler handler, size_t, PgSQLHeader, PgSQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 1) && is(ParameterTypeTuple!(RowHandler)[0] == PgSQLRow))
    {
        static if (is(ReturnType!(RowHandler) == void))
        {
            handler(row);
            return true;
        }
        else
        {
            return handler(row); // return type must be bool
        }
    }

    bool callHandler(RowHandler)(RowHandler handler, size_t i, PgSQLHeader, PgSQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 2) && isNumeric!(ParameterTypeTuple!(RowHandler)[0]) && is(ParameterTypeTuple!(RowHandler)[1] == PgSQLRow))
    {
        static if (is(ReturnType!(RowHandler) == void))
        {
            handler(cast(ParameterTypeTuple!(RowHandler)[0])i, row);
            return true;
        }
        else
        {
            return handler(cast(ParameterTypeTuple!(RowHandler)[0])i, row); // return type must be bool
        }
    }

    bool callHandler(RowHandler)(RowHandler handler, size_t, PgSQLHeader header, PgSQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 2) && is(ParameterTypeTuple!(RowHandler)[0] == PgSQLHeader) && is(ParameterTypeTuple!(RowHandler)[1] == PgSQLRow))
    {
        static if (is(ReturnType!(RowHandler) == void))
        {
            handler(header, row);
            return true;
        }
        else
        {
            return handler(header, row); // return type must be bool
        }
    }

    bool callHandler(RowHandler)(RowHandler handler, size_t i, PgSQLHeader header, PgSQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 3) && isNumeric!(ParameterTypeTuple!(RowHandler)[0]) && is(ParameterTypeTuple!(RowHandler)[1] == PgSQLHeader) && is(ParameterTypeTuple!(RowHandler)[2] == PgSQLRow))
    {
        static if (is(ReturnType!(RowHandler) == void))
        {
            handler(i, header, row);
            return true;
        }
        else
        {
            return handler(i, header, row); // return type must be bool
        }
    }

    void resultSetRowText(InputPacket packet, PgSQLHeader header, ref PgSQLRow row)
    {
        assert(row.columns.length == header.length);

        assert(packet.type == InputMessageType.DataRow);
        const rowlen = packet.eat!ushort();

        foreach(i, ref column; header)
        {
            if (i < rowlen)
            {
                if (column.format == FormatCode.Text)
                {
                    eatValueText(packet, column, row.get_(i));
                }
                else
                {
                    assert(false);
                }
            }
            else
            {
                row.get_(i) = PgSQLValue(column.name, PgColumnTypes.NULL, null, 0);
            }
        }
        assert(packet.empty);
    }

    void resultSetText(RowHandler)(InputPacket packet, RowHandler handler)
    {
        columns_.length = 0;

        auto columns = cast(size_t)packet.eat!ushort;

        columnDefs(columns, header_, packet);
        row_.header_(header_);

        size_t index;
        while (true)
        {
            auto row = retrieve();
            if (isStatus(row))
            {
                eatStatuses(row);
                break;
            }

            resultSetRowText(row, header_, row_);
            if (!callHandler(handler, index++, header_, row_))
            {
                discardUntilStatus();
                break;
            }
        }
    }

    void discardAll(InputPacket packet)
    {
        auto columns = cast(size_t)packet.eatLenEnc();
        columnDefs(columns, header_, packet);

        discardUntilStatus();
    }

    void discardUntilStatus()
    {
        while (true)
        {
            auto row = retrieve();
            if (isStatus(row))
            {
                eatStatuses(row);
                break;
            }
        }
    }

    auto prepareSQL(Args...)(const(char)[] sql, Args args)
    {
        auto estimated = sql.length;
        size_t argCount;

        foreach(i, arg; args)
        {
            static if (is(typeof(arg) == typeof(null)))
            {
                ++argCount;
                estimated += 4;
            }
            else static if (is(Unqual!(typeof(arg)) == PgSQLValue))
            {
                ++argCount;
                final switch(arg.type) with (PgColumnTypes)
                {
                    case NULL:
                        estimated += 4;
                        break;
                    case CHAR:
                        estimated += 2;
                        break;
                    case BOOL:
                        estimated += 2;
                        break;
                    case INT2:
                        estimated += 6;
                        break;
                    case INT4:
                        estimated += 7;
                        break;
                    case INT8:
                        estimated += 15;
                        break;
                    case REAL:
                    case DOUBLE:
                        estimated += 8;
                        break;
                    case UNKNOWN:
                    case MONEY:
                    case POINT:
                    case LINE:
                    case LSEG:
                    case PATH:
                    case POLYGON:
                    case TINTERVAL:
                    case CIRCLE:
                    case BOX:
                    case JSON:
                    case JSONB:
                    case XML:
                    case MACADDR:
                    case MACADDR8:
                    case INET:
                    case CIDR:
                    case NAME:
                    case TEXT:
                    case INTERVAL:
                    case BIT:
                    case VARBIT:
                    case NUMERIC:
                    case UUID:
                    case CHARA:
                    case BYTEA:
                    case VARCHAR:
                        estimated += 4 + arg.peek!(const(char)[]).length;
                        break;
                    case DATE:
                        estimated += 10;
                        break;
                    case TIME:
                    case TIMETZ:
                        estimated += 22;
                        break;
                    case TIMESTAMP:
                    case TIMESTAMPTZ:
                        estimated += 30;
                        break;
                }
            }
            else static if (isArray!(typeof(arg)) && !isSomeString!(typeof(arg)))
            {
                argCount += arg.length;
                estimated += arg.length * 6;
            }
            else static if (isSomeString!(typeof(arg)) || is(Unqual!(typeof(arg)) == PgSQLRawString) || is(Unqual!(typeof(arg)) == PgSQLFragment) || is(Unqual!(typeof(arg)) == PgSQLBinary))
            {
                ++argCount;
                estimated += 2 + arg.length;
            }
            else
            {
                ++argCount;
                estimated += 6;
            }
        }

        sql_.clear;
        sql_.reserve(max(8192, estimated));

        alias AppendFunc = bool function(ref Appender!(char[]), ref const(char)[] sql, ref size_t, const(void)*) @safe pure nothrow;
        AppendFunc[Args.length] funcs;
        const(void)*[Args.length] addrs;

        foreach (i, Arg; Args)
        {
            static if (is(Arg == enum))
            {
                funcs[i] = () @trusted { return cast(AppendFunc)&appendNextValue!(OriginalType!Arg); }();
                addrs[i] = (ref x) @trusted { return cast(const void*)&x; }(cast(OriginalType!(Unqual!Arg))args[i]);
            }
            else
            {
                funcs[i] = () @trusted { return cast(AppendFunc)&appendNextValue!(Arg); }();
                addrs[i] = (ref x) @trusted { return cast(const void*)&x; }(args[i]);
            }
        }

        size_t indexArg;
        foreach (i; 0..Args.length)
        {
            if (!funcs[i](sql_, sql, indexArg, addrs[i]))
                throw new PgSQLErrorException(format("Wrong number of parameters for query. Got %d but expected %d.", argCount, indexArg));
        }

        if (copyUpToNext(sql_, sql))
        {
            ++indexArg;
            while (copyUpToNext(sql_, sql))
                ++indexArg;
            throw new PgSQLErrorException(format("Wrong number of parameters for query. Got %d but expected %d.", argCount, indexArg));
        }

        return sql_.data;
    }

    Socket socket_;
    PgSQLHeader header_;
    PgSQLRow row_;
    char[] columns_;
    char[] schema_;
    ubyte[] in_;
    ubyte[] out_;
    ubyte seq_;
    Appender!(char[]) sql_;

    ConnectionStatus status_;
    ConnectionSettings settings_;
    ServerInfo server_;

    ConnectionNotice[] notices_;

    bool busy_;
    bool pooled_;
    DateTime releaseTime_;
}

private auto copyUpToNext(ref Appender!(char[]) app, ref const(char)[] sql)
{
    size_t offset;
    dchar quote = '\0';

    while (offset < sql.length)
    {
        auto ch = decode!(UseReplacementDchar.no)(sql, offset);
        switch (ch)
        {
            case '?':
                if (!quote)
                {
                    app.put(sql[0..offset - 1]);
                    sql = sql[offset..$];
                    return true;
                }
                else
                {
                    goto default;
                }
            case '\'':
            case '\"':
            case '`':
                if (quote == ch)
                {
                    quote = '\0';
                }
                else if (!quote)
                {
                    quote = ch;
                }
                goto default;
            case '\\':
                if (quote && (offset < sql.length))
                    decode!(UseReplacementDchar.no)(sql, offset);
                goto default;
            default:
                break;
        }
    }
    app.put(sql[0..offset]);
    sql = sql[offset..$];
    return false;
}

private bool appendNextValue(T)(ref Appender!(char[]) app, ref const(char)[] sql, ref size_t indexArg, const(void)* arg)
{
    static if (isArray!T && !isSomeString!(OriginalType!T))
    {
        foreach (i, ref v; *cast(T*)arg)
        {
            if (copyUpToNext(app, sql))
            {
                appendValue(app, v);
                ++indexArg;
            }
            else
            {
                return false;
            }
        }
    }
    else
    {
        if (copyUpToNext(app, sql))
        {
            appendValue(app, *cast(T*)arg);
            ++indexArg;
        }
        else
        {
            return false;
        }
    }

    return true;
}

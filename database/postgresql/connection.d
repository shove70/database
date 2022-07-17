module database.postgresql.connection;

// dfmt off
import database.postgresql.appender,
	database.postgresql.db,
	database.postgresql.packet,
	database.postgresql.protocol,
	database.postgresql.row,
	database.postgresql.type,
	database.util,
	std.algorithm,
	std.array,
	std.experimental.logger,
	std.format,
	std.string,
	std.traits;
import std.regex : ctRegex, matchFirst;
import std.uni : sicmp;
import std.utf : decode, UseReplacementDchar;
// dfmt on

alias Socket = DBSocket!PgSQLConnectionException;

struct Status {
	bool ready;
	TransactionStatus transaction = TransactionStatus.Idle;

	ulong affected, insertID;
}

struct Settings {
	string
		host,
		user,
		pwd,
		db;
	ushort port = 5432;
}

private struct ServerInfo {
	string
		versionStr,
		encoding,
		application,
		timeZone;

	uint processId,
		 cancellationKey;
}

class Connection {
	import std.datetime;

	this(Settings settings)
	{
		settings_ = settings;
		connect();
	}

	this(string host, string user, string pwd, string db, ushort port = 5432) {
		this(Settings(host, user, pwd, db, port));
	}

	void ping()
	{
	}

	@property pure nothrow @nogc {
		string schema() const { return cast(string)schema_; }

		auto settings() const { return settings_; }

		auto notices() const { return notices_; }
	}

	bool exec(Args...)(string sql, Args args) {
		try {
			query(sql, args);
			return true;
		} catch(DBException)
			return false;
	}

	auto query(T = PgSQLRow, Args...)(string sql, Args args) {
		// scope(failure) disconnect();

		send(prepareSQL(sql, args));

		return QueryResult!T(this);
	}

	void set(T)(string variable, T value) {
		query("set session ?=?", PgSQLFragment(variable), value);
	}

	T get(T = string)(string variable) {
		return query!T("show session variables like ?", variable).get;
	}

	bool begin() {
		if (inTransaction)
			throw new PgSQLErrorException("PgSQL doesn't support nested transactions - commit or rollback before starting a new transaction");

		query("begin");
		return inTransaction;
	}

	bool commit() {
		if (!inTransaction)
			throw new PgSQLErrorException("No active transaction");

		query("commit");
		return !inTransaction;
	}

	bool rollback() {
		if (!inTransaction)
			throw new PgSQLErrorException("No active transaction");

		query("rollback");
		return !inTransaction;
	}

	alias OnDisconnectCallback = void delegate();

	@property {
		bool inTransaction() const {
			return connected && status_.transaction == TransactionStatus.Inside;
		}

		ulong insertID() const nothrow @nogc { return status_.insertID; }

		ulong affected() const nothrow @nogc { return status_.affected; }

		bool connected() const { return socket.connected; }
	}

	void close() nothrow @nogc { socket.close(); }

	void reuse() {
		onDisconnect = null;

		ensureConnected();

		if (inTransaction)
			rollback;
	}

package(database):

	bool busy, pooled;
	DateTime releaseTime;

private:
	void disconnect() {
		close();
		if (onDisconnect)
			onDisconnect();
	}

	void connect() {
		socket.connect(settings_.host, settings_.port);

		auto startup = OutputPacket(&out_);
		startup.put(0x00030000u);
		startup.putz("user");
		startup.putz(settings_.user);
		if (settings_.db != "") {
			startup.putz("database");
			startup.putz(settings_.db);
		}
		startup.put!ubyte(0);
		startup.finalize();

		socket.write(startup.data);

		if (eatAuth(retrieve()))
			eatAuth(retrieve());
		eatStatuses(retrieve());
	}

	void send(Args...)(Args args) {
		ensureConnected();

		auto cmd = OutputPacket(OutputMessageType.Query, &out_);
		static foreach (arg; args)
			cmd.put!(const(char[]))(arg);
		cmd.put!ubyte(0);
		cmd.finalize();

		socket.write(cmd.data);
	}

	void ensureConnected() {
		if (!socket.connected)
			connect();
	}

	InputPacket retrieve(ubyte control) {
		scope(failure) disconnect();

		ubyte[4] header = void;
		socket.read(header);

		auto len = native(cast(uint*)header.ptr) - 4;
		in_.length = len;
		socket.read(in_);

		if (in_.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(control, in_);
	}

	package InputPacket retrieve() {
		scope(failure) disconnect();

		ubyte[5] header = void;
		socket.read(header);

		auto len = native!uint(cast(uint*)&header[1]) - 4;
		in_.length = len;
		socket.read(in_);

		if (in_.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(header[0], in_);
	}

	package auto eatStatuses(InputPacket packet) {
		notices_.length = 0;

		for(; ;) {
			auto status = eatStatus(packet);
			if (status == InputMessageType.ReadyForQuery)
				return status;
			packet = retrieve();
			if (packet.type == InputMessageType.RowDescription)
				return InputMessageType.RowDescription;
		}
	}

	bool eatAuth(InputPacket packet) {
		scope(failure) disconnect();

		auto type = cast(InputMessageType)packet.type;

		switch (type) with (InputMessageType) {
		case Authentication:
			auto auth = packet.eat!uint;
			auto reply = OutputPacket(OutputMessageType.PasswordMessage, &out_);

			switch (auth) {
			case 0:
				return false;
			case 3:
				reply.putz(settings_.pwd);
				break;
			version (NoMD5Auth) {} else {
				case 5:
					static char[32] MD5toHex(T...)(in T data) {
						import std.ascii : LetterCase;
						import std.digest.md : md5Of, toHexString;
						return md5Of(data).toHexString!(LetterCase.lower);
					}

					auto salt = packet.eat!(ubyte[])(4);
					reply.put("md5");
					reply.putz(MD5toHex(MD5toHex(settings_.pwd, settings_.user), salt));
					break;
			}
			/+case 6: // SCM
			case 7: // GSS
			case 8:
			case 9:
			case 10: // SASL
			case 11:
			case 12:+/
			default:
				throw new PgSQLProtocolException("Unsupported authentication method: %s".format(auth));
			}

			reply.finalize();
			socket.write(reply.data);
			break;
		case NoticeResponse:
			eatNoticeResponse(packet);
			break;
		case ErrorResponse:
			eatNoticeResponse(packet);
			throwErr(true);
			break;
		default:
			throw new PgSQLProtocolException("Unexpected message: %s".format(type));
		}

		return true;
	}

	void eatParameterStatus(InputPacket packet)
	in(packet.type == InputMessageType.ParameterStatus)
	out(; packet.empty) {
		auto name = packet.eatz();
		auto value = packet.eatz();

		//info("parameter ", name, " = ", value);

		switch (hashOf(name)) {
		case hashOf("server_version"): server.versionStr = value.dup; break;
		case hashOf("server_encoding"): server.encoding = value.dup; break;
		case hashOf("application_name"): server.application = value.dup; break;
		case hashOf("TimeZone"): server.timeZone = value.dup; break;
		default:
		}
	}

	void eatBackendKeyData(InputPacket packet)
	in(packet.type == InputMessageType.BackendKeyData) {
		server.processId = packet.eat!uint;
		server.cancellationKey = packet.eat!uint;
	}

	void eatNoticeResponse(InputPacket packet)
	in(packet.type == InputMessageType.NoticeResponse || packet.type == InputMessageType.ErrorResponse) {
		Notice notice;
		auto field = packet.eat!ubyte;
		while (field) {
			auto value = packet.eatz();

			switch (field) with (NoticeMessageField) {
			case Severity:
			case SeverityLocal:
				switch (hashOf(value)) with (Notice.Severity) {
				case hashOf("ERROR"): notice.severity = ERROR; break;
				case hashOf("FATAL"): notice.severity = FATAL; break;
				case hashOf("PANIC"): notice.severity = PANIC; break;
				case hashOf("WARNING"): notice.severity = WARNING; break;
				case hashOf("DEBUG"): notice.severity = DEBUG; break;
				case hashOf("INFO"): notice.severity = INFO; break;
				case hashOf("LOG"): notice.severity = LOG; break;
				default:
				}
				break;
			case Code: notice.code = value.idup; break;
			case Message: notice.message = value.idup; break;
			case Detail: notice.detail = value.idup; break;
			case Hint: notice.hint = value.idup; break;
			case Position: notice.position = value.parse!uint; break;
			case Where: notice.where = value.idup; break;
			case Schema: notice.schema = value.idup; break;
			case Table: notice.table = value.idup; break;
			case Column: notice.column = value.idup; break;
			case DataType: notice.type = value.idup; break;
			case Constraint: notice.constraint = value.idup; break;
			case File:
			case Line:
			case Routine:
				break;
			default:
				info("pgsql notice: ", cast(char)field, ' ', value);
			}
			field = packet.eat!ubyte;
		}

		notices_ ~= notice;
	}

	void eatCommandComplete(InputPacket packet)
	in(packet.type == InputMessageType.CommandComplete) {
		auto tag = packet.eatz();
		auto p = tag.indexOf(' ') + 1;
		auto cmd = tag[0 .. p];
		if (p)
			tag = tag[p .. $];
		else
			swap(tag, cmd);

		switch (hashOf(cmd)) {
		case hashOf("INSERT"):
			status_.insertID = tag.parse!ulong;
			status_.affected = tag.parse!ulong(1);
			break;
		case hashOf("SELECT"), hashOf("DELETE"), hashOf("UPDATE"),
			hashOf("MOVE"), hashOf("FETCH"), hashOf("COPY"):
			status_.insertID = 0;
			status_.affected = tag == "" ? 0 : tag.parse!ulong;
			break;
		case hashOf("CREATE"), hashOf("DROP"):
			status_.insertID = 0;
			break;
		default:
		}
	}

	auto eatStatus(InputPacket packet) {
		auto type = cast(InputMessageType)packet.type;

		switch (type) with (InputMessageType) {
		case ParameterStatus:
			eatParameterStatus(packet); break;
		case BackendKeyData:
			eatBackendKeyData(packet); break;
		case ReadyForQuery:
			status_.transaction = cast(TransactionStatus)packet.eat!ubyte;
			status_.ready = true;
			break;
		case NoticeResponse:
			eatNoticeResponse(packet); break;
		case ErrorResponse:
			eatNoticeResponse(packet);
			throwErr(true);
			break;
		case CommandComplete:
			eatCommandComplete(packet); break;
		default:
			throw new PgSQLProtocolException("Unexpected message: %s".format(type));
		}

		return type;
	}

	void throwErr(bool force) {
		foreach (ref notice; notices_)
			switch (notice.severity) with (Notice.Severity) {
			case PANIC, ERROR, FATAL:
				throw new PgSQLErrorException(cast(string)notice.message);
			default:
			}

		if (force)
			throw new PgSQLErrorException(cast(string)notices_.front.message);
	}

	auto prepareSQL(Args...)(string sql, Args args) {
		auto estimated = sql.length;
		size_t argCount;

		static foreach(arg; args) {{
			alias T = typeof(arg);
			static if (is(T == typeof(null))) {
				++argCount;
				estimated += 4;
			} else static if (is(Unqual!T == PgSQLValue)) {
				++argCount;
				switch(arg.type) with (PgType) {
				case NULL: estimated += 4; break;
				case CHAR: estimated += 2; break;
				case BOOL: estimated += 2; break;
				case INT2: estimated += 6; break;
				case INT4: estimated += 7; break;
				case INT8: estimated += 15; break;
				case REAL, DOUBLE:
					estimated += 8;
					break;
				case DATE:
					estimated += 10;
					break;
				case TIME, TIMETZ:
					estimated += 22;
					break;
				case TIMESTAMP, TIMESTAMPTZ:
					estimated += 30;
					break;
				default:
					estimated += 4 + arg.peek!string.length;
				}
			} else static if (isSomeString!T || is(Unqual!T == PgSQLRawString) ||
				is(Unqual!T == PgSQLFragment) || is(Unqual!T == ubyte[])) {
				++argCount;
				estimated += 2 + arg.length;
			} else static if (isArray!T) {
				argCount += arg.length;
				estimated += arg.length * 6;
			} else {
				++argCount;
				estimated += 6;
			}
		}}

		sql_.clear;
		sql_.reserve(max(8192, estimated));

		size_t indexArg;
		foreach (i, Arg; Args) {
			static if (is(Arg == enum))
				alias func = appendNextValue!(OriginalType!Arg);
			else
				alias func = appendNextValue!Arg;
			if (!func(sql_, sql, indexArg, &args[i]))
				throw new PgSQLErrorException("Wrong number of parameters for query. Got %d but expected %d.".format(argCount, indexArg));
		}

		if (copyUpToNext(sql_, sql)) {
			++indexArg;
			while (copyUpToNext(sql_, sql))
				++indexArg;
			throw new PgSQLErrorException("Wrong number of parameters for query. Got %d but expected %d.".format(argCount, indexArg));
		}

		return sql_[];
	}

	Socket socket;
	char[] schema_;
	ubyte[] in_, out_;
	ubyte seq_;
	Appender!(char[]) sql_;

	OnDisconnectCallback onDisconnect;
	Status status_;
	Settings settings_;
	ServerInfo server;

	Notice[] notices_;
}

private:

void skipColumnDef(ref InputPacket packet) {
	packet.skipz();
	packet.skip(18);
}

auto copyUpToNext(ref Appender!(char[]) app, ref string sql) {
	size_t offset;
	dchar quote = '\0';

	while (offset < sql.length) {
		auto ch = decode!(UseReplacementDchar.no)(sql, offset);
		switch (ch) {
		case '?':
			if (quote)
				goto default;
			app ~= sql[0..offset - 1];
			sql = sql[offset..$];
			return true;
		case '\'':
		case '\"':
		case '`':
			if (quote == ch)
				quote = '\0';
			else if (!quote)
				quote = ch;
			goto default;
		case '\\':
			if (quote && offset < sql.length)
				decode!(UseReplacementDchar.no)(sql, offset);
			break;
		default:
		}
	}
	app ~= sql[0..offset];
	sql = sql[offset..$];
	return false;
}

bool appendNextValue(T)(ref Appender!(char[]) app, ref string sql, ref size_t indexArg, const(void)* arg) {
	static if (isArray!T && !isSomeString!(OriginalType!T) && !is(Unqual!T == ubyte[])) {
		foreach (i, ref v; *cast(T*)arg) {
			if (!copyUpToNext(app, sql))
				return false;
			appendValue(app, v);
			++indexArg;
		}
	} else {
		if (!copyUpToNext(app, sql))
			return false;
		appendValue(app, *cast(T*)arg);
		++indexArg;
	}
	return true;
}

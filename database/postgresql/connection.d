module database.postgresql.connection;

// dfmt off
import database.postgresql.db,
	database.postgresql.packet,
	database.postgresql.protocol,
	database.postgresql.row,
	database.postgresql.type,
	database.util;
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

@safe:
class Connection {
	import std.datetime,
	std.format,
	std.functional,
	std.logger,
	std.range,
	std.string,
	std.traits;

	Socket socket;

	this(Settings settings) {
		settings_ = settings;
		connect();
	}

	this(string host, string user, string pwd, string db, ushort port = 5432) {
		this(Settings(host, user, pwd, db, port));
	}

	void ping() {
	}

	@property {
		auto settings() const => settings_;

		auto notices() const => notices_;

		auto notifications() const => notifications_;
	}

	auto runSql(T = PgSQLRow)(in char[] sql) {
		ensureConnected();

		out_.length = 1 + 4 + sql.length + 1;
		auto cmd = OutputPacket(OutputMessageType.Query, out_);
		cmd.put(sql);
		socket.write(cmd.data);
		return QueryResult!T(this);
	}

	auto query(T = PgSQLRow, Args...)(in char[] sql, auto ref Args args) {
		prepare!Args("", sql);
		bind("", "", forward!args);
		flush();
		eatStatuses(InputMessageType.BindComplete, true);
		describe();
		sendExecute();
		return QueryResult!T(this, FormatCode.Binary);
	}

	ulong exec(Args...)(in char[] sql, auto ref Args args) {
		prepare!Args("", sql);
		bind("", "", forward!args);
		flush();
		eatStatuses(InputMessageType.BindComplete, true);
		sendExecute();
		eatStatuses();
		return affected;
	}

	void prepare(Args...)(in char[] statement, in char[] sql)
	if (Args.length <= short.max) {
		ensureConnected();

		out_.length = 4 +
			statement.length + 1 +
			sql.length + 1 +
			2 +
			Args.length * 4 + 1;
		auto cmd = OutputPacket(OutputMessageType.Parse, out_);
		cmd.put(statement);
		cmd.put(sql);
		cmd.put(cast(short)Args.length);
		foreach (T; Args)
			cmd.put(PgTypeof!T);
		socket.write(cmd.data);
	}

	void bind(Args...)(in char[] portal, in char[] statement, auto ref Args args) @trusted
	if (Args.length <= short.max) {
		out_.length = 4 +
			portal.length + 1 +
			statement.length + 1 +
			2 + 2 * Args.length +
			2 + 4 * Args.length +
			2 + 2;
		auto cmd = OutputPacket(OutputMessageType.Bind, out_);
		cmd.put(portal);
		cmd.put(statement);
		cmd.put!short(1);
		cmd.put(FormatCode.Binary);
		cmd.put(cast(short)Args.length);
		foreach (i, ref arg; args) {
			enum U = PgTypeof!(Args[i]);
			static assert(U != PgType.UNKNOWN, "Unrecognized type " ~ Args[i].stringof);
			static if (is(U == PgType.NULL))
				cmd.put(-1);
			else static if (isArray!(Args[i])) {
				if (arg.length >= int.max)
					throw new PgSQLException("array is too long to serialize");
				cmd.put(cast(int)arg.length);
				cmd.put(cast(ubyte[])arg);
			} else {
				cmd.put(cast(int)arg.sizeof);
				cmd.put(arg);
			}
		}
		cmd.put!short(1);
		cmd.put(FormatCode.Binary);
		socket.write(cmd.data);
	}

	void describe(in char[] name = "", DescribeType type = DescribeType.Statement) {
		out_.length = 1 + 4 + 1 + name.length + 1;
		auto cmd = OutputPacket(OutputMessageType.Describe, out_);
		cmd.put(type);
		cmd.put(name);
		socket.write(cmd.data);
	}

	void flush() {
		enum ubyte[5] buf = [OutputMessageType.Flush, 0, 0, 0, 4];
		socket.write(buf);
	}

	void sync() {
		enum ubyte[5] buf = [OutputMessageType.Sync, 0, 0, 0, 4];
		socket.write(buf);
	}

	ulong executePortal(string portal = "", int rowLimit = 0) {
		sendExecute(portal, rowLimit);
		eatStatuses();
		return affected;
	}

	void cancel(uint processId, uint cancellationKey) @trusted {
		ubyte[16] buf = [16, 0, 0, 0, 4, 210, 22, 46, 0, 0, 0, 0, 0, 0, 0, 0];
		*cast(uint*)&buf[8] = native(processId);
		*cast(uint*)&buf[12] = native(cancellationKey);
		socket.write(buf);
	}

	void close(DescribeType type, in char[] name = "") {
		out_.length = 1 + 4 + 1 + name.length + 1;
		auto cmd = OutputPacket(OutputMessageType.Close, out_);
		cmd.put(type);
		cmd.put(name);
		socket.write(cmd.data);
		flush();
		eatStatuses(InputMessageType.CloseComplete);
	}

	bool begin() {
		if (inTransaction)
			throw new PgSQLErrorException(
				"PgSQL doesn't support nested transactions" ~
					"- commit or rollback before starting a new transaction");

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
		bool inTransaction() const => connected && status_.transaction == TransactionStatus.Inside;

		ulong insertID() const nothrow @nogc => status_.insertID;

		ulong affected() const nothrow @nogc => status_.affected;

		bool ready() const nothrow @nogc => status_.ready;

		bool connected() const => socket && socket.isAlive;
	}

	void close(bool sendTerminate = true) nothrow {
		scope (exit) {
			socket.close();
			socket = null;
		}
		enum ubyte[5] terminateMsg = [OutputMessageType.Terminate, 0, 0, 0, 4];
		if (sendTerminate)
			try
				socket.write(terminateMsg);
			catch (Exception) {
			}
	}

	void reuse() {
		onDisconnect = null;
		ensureConnected();

		if (inTransaction)
			rollback();
	}

package(database):

	bool busy, pooled;
	DateTime releaseTime;

private:
	void disconnect() {
		close(false);
		if (onDisconnect)
			onDisconnect();
	}

	void connect() {
		socket = new Socket(settings_.host, settings_.port);

		out_.length = 4 + 4 +
			"user".length + 1 + settings_.user.length + 1 +
			(settings_.db != "" ? "database".length + 1 + settings_.db.length + 1 : 0) + 1;
		auto startup = OutputPacket(out_);
		startup.put(0x00030000u);
		startup.put("user");
		startup.put(settings_.user);
		if (settings_.db != "") {
			startup.put("database");
			startup.put(settings_.db);
		}
		startup.put!ubyte(0);
		socket.write(startup.data);

		if (eatAuth(retrieve()))
			eatAuth(retrieve());
		eatBackendKeyData(eatStatuses(InputMessageType.BackendKeyData));
		eatStatuses();
	}

	void sendExecute(string portal = "", int rowLimit = 0) {
		out_.length = 1 + 4 + portal.length + 1 + 4;
		auto cmd = OutputPacket(OutputMessageType.Execute, out_);
		cmd.put(portal);
		cmd.put(rowLimit);
		socket.write(cmd.data);
		sync();
	}

	void ensureConnected() {
		if (!connected)
			connect();
	}

	InputPacket retrieve(ubyte control) @trusted {
		scope (failure)
			disconnect();

		uint[1] header = void;
		socket.read(header);

		auto len = native(header[0]) - 4;
		in_.length = len;
		socket.read(in_);

		if (in_.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(control, in_);
	}

	package InputPacket retrieve() @trusted {
		scope (failure)
			disconnect();

		ubyte[5] header = void;
		socket.read(header);

		uint len = native(*cast(uint*)&header[1]) - 4;
		in_.length = len;
		socket.read(in_);

		if (in_.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(header[0], in_);
	}

	package void eatStatuses() {
		while (eatStatus(retrieve()) != InputMessageType.ReadyForQuery) {
		}
	}

	package auto eatStatuses(InputMessageType type, bool syncOnError = false) @trusted {
		InputPacket packet = void;
		do {
			packet = retrieve();
			if (packet.type == type)
				break;
		}
		while (eatStatus(packet, syncOnError) != InputMessageType.ReadyForQuery);
		return packet;
	}

	bool eatAuth(InputPacket packet) {
		import std.algorithm : max;

		scope (failure)
			disconnect();

		auto type = cast(InputMessageType)packet.type;
		switch (type) with (InputMessageType) {
		case Authentication:
			auto auth = packet.eat!uint;
			version (NoMD5Auth)
				out_.length = 1 + 4 + settings_.pwd.length + 1;
			else
				out_.length = 1 + 4 + max(settings_.pwd.length, 3 + 32) + 1; // 3 for md5 and 32 is hash size
			auto reply = OutputPacket(OutputMessageType.PasswordMessage, out_);

			switch (auth) {
			case 0:
				return false;
			case 3:
				reply.put(settings_.pwd);
				break;
				version (NoMD5Auth) {
				} else {
			case 5:
					static char[32] MD5toHex(T...)(in T data) {
						import std.ascii : LetterCase;
						import std.digest.md : md5Of, toHexString;

						return md5Of(data).toHexString!(LetterCase.lower);
					}

					auto salt = packet.eat!(ubyte[])(4);
					reply.put("md5".representation);
					reply.put(MD5toHex(MD5toHex(settings_.pwd, settings_.user), salt));
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
				throw new PgSQLProtocolException(
					"Unsupported authentication method: %s".format(auth));
			}

			socket.write(reply.data);
			break;
		case NoticeResponse:
			eatNoticeResponse(packet);
			break;
		case ErrorResponse:
			eatNoticeResponse(packet);
			throwErr();
		default:
			throw new PgSQLProtocolException("Unexpected message: %s".format(type));
		}

		return true;
	}

	void eatParameterStatus(ref InputPacket packet)
	in (packet.type == InputMessageType.ParameterStatus)
	out (; packet.empty) {
		auto name = packet.eatz();
		auto value = packet.eatz();
		//info("parameter ", name, " = ", value);
		// dfmt off
		switch (hashOf(name)) {
		case hashOf("server_version"): server.versionStr = value.dup; break;
		case hashOf("server_encoding"): server.encoding = value.dup; break;
		case hashOf("application_name"): server.application = value.dup; break;
		case hashOf("TimeZone"): server.timeZone = value.dup; break;
		default:
		}
		// dfmt on
	}

	void eatBackendKeyData(InputPacket packet)
	in (packet.type == InputMessageType.BackendKeyData) {
		server.processId = packet.eat!uint;
		server.cancellationKey = packet.eat!uint;
	}

	void eatNoticeResponse(ref InputPacket packet)
	in (packet.type == InputMessageType.NoticeResponse || packet.type == InputMessageType
		.ErrorResponse) {
		Notice notice;
		auto field = packet.eat!ubyte;
		// dfmt off
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
			case Code: notice.code = value; break;
			case Message: notice.message = value.idup; break;
			case Detail: notice.detail = value.idup; break;
			case Hint: notice.hint = value.idup; break;
			case Position: notice.position = value.parse!uint; break;
			case InternalPosition: notice.internalPos = value.idup; break;
			case InternalQuery: notice.internalQuery = value.idup; break;
			case Where: notice.where = value.idup; break;
			case Schema: notice.schema = value.idup; break;
			case Table: notice.table = value.idup; break;
			case Column: notice.column = value.idup; break;
			case DataType: notice.type = value.idup; break;
			case Constraint: notice.constraint = value.idup; break;
			case File: notice.file = value.idup; break;
			case Line: notice.line = value.idup; break;
			case Routine: notice.routine = value.idup; break;
			default:
				info("pgsql notice: ", cast(char)field, ' ', value);
			}
			field = packet.eat!ubyte;
		}
		// dfmt on
		notices_ ~= notice;
	}

	void eatNotification(ref InputPacket packet)
	in (packet.type == InputMessageType.NotificationResponse) {
		notifications_ ~= Notification(packet.eat!int, packet.eatz(), packet.eatz());
	}

	void eatCommandComplete(ref InputPacket packet)
	in (packet.type == InputMessageType.CommandComplete) {
		import std.algorithm : swap;

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
			status_.affected = tag.parse!ulong;
			break;
		case hashOf("CREATE"), hashOf("DROP"):
			status_.insertID = 0;
			status_.affected = 0;
			break;
		default:
		}
	}

	auto eatStatus(InputPacket packet, bool syncOnError = false) {
		auto type = cast(InputMessageType)packet.type;
		switch (type) with (InputMessageType) {
		case ParameterStatus:
			eatParameterStatus(packet);
			break;
		case ReadyForQuery:
			notices_.length = 0;
			status_.transaction = packet.eat!TransactionStatus;
			status_.ready = true;
			break;
		case NoticeResponse:
			eatNoticeResponse(packet);
			break;
		case ErrorResponse:
			if (syncOnError)
				sync();
			eatNoticeResponse(packet);
			throwErr();
		case NotificationResponse:
			eatNotification(packet);
			break;
		case CommandComplete:
			eatCommandComplete(packet);
			break;
		case EmptyQueryResponse, NoData, ParameterDescription, ParseComplete, BindComplete, PortalSuspended:
			break;
		default:
			throw new PgSQLProtocolException("Unexpected message: %s".format(type));
		}
		return type;
	}

	noreturn throwErr() {
		foreach (ref notice; notices_) switch (notice.severity) with (Notice.Severity) {
		case PANIC, ERROR, FATAL:
			throw new PgSQLErrorException(notice.message);
		default:
		}

		throw new PgSQLErrorException(notices_.front.message);
	}

	ubyte[] in_, out_;

	OnDisconnectCallback onDisconnect;
	Status status_;
	Settings settings_;
	ServerInfo server;
	Notice[] notices_;
	Notification[] notifications_;
}

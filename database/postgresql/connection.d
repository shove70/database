module database.postgresql.connection;

import database.postgresql.db,
database.postgresql.packet,
database.postgresql.protocol,
database.postgresql.row,
database.postgresql.type,
database.util;

alias Socket = DBSocket!PgSQLConnectionException;

/++ Runtime state captured from the server for the most recent command. +/
struct Status {
	bool ready;
	TransactionStatus transaction = TransactionStatus.Idle;

	ulong affected, insertId;
}

// dfmt off
/++ Connection settings passed to `Connection` on startup. +/
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
// dfmt on

@safe:
class Connection {
	import std.datetime,
	std.functional,
	std.logger,
	std.range,
	std.string,
	std.traits,
	std.conv : text;

	Socket socket;

	/++ Construct a PostgreSQL connection from a prepared `Settings` value. +/
	this(Settings settings) {
		settings_ = settings;
		connect();
	}

	/++ Construct a PostgreSQL connection from primitive connection parameters. +/
	this(string host, string user, string pwd, string db, ushort port = 5432) {
		this(Settings(host, user, pwd, db, port));
	}

	/++ Send a lightweight protocol-level ping request (currently no-op). +/
	void ping() {
	}

	/++ Callback invoked when the socket is disconnected unexpectedly. +/
	alias OnDisconnectCallback = void delegate();

	@property final {
		/++ Whether the connection is currently inside an open transaction block. +/
		bool inTransaction() const => connected && status_.transaction == TransactionStatus.Inside;

		/++ Last inserted identity value, if available. +/
		ulong insertId() const nothrow @nogc => status_.insertId;

		/++ Rows affected by the last command, if reported by PostgreSQL. +/
		ulong affected() const nothrow @nogc => status_.affected;

		/++ Whether the backend reported `ReadyForQuery` after latest command. +/
		bool ready() const nothrow @nogc => status_.ready;

		/++ Whether the underlying socket exists and is alive. +/
		bool connected() const => socket && socket.isAlive;

		/++ Current effective connection settings. +/
		auto settings() const => settings_;

		/++ Collected server notices from the last command cycle. +/
		auto notices() const => notices_;

		/++ Collected asynchronous notifications received from the server. +/
		auto notifications() const => notifications_;
	}

	/++ Execute SQL that returns a result set and return a `QueryResult` cursor. +/
	auto runSql(T = PgSQLRow)(in char[] sql) @trusted {
		ensureConnected();

		auto len = 5 + sql.length + 1;
		mixin Output!(len, OMT.Query);
		op.put(sql);
		socket.write(op.data);
		return QueryResult!T(this);
	}

	/++ Execute a parameterized SQL statement and return its first result cursor. +/
	auto query(T = PgSQLRow, Args...)(in char[] sql, Args args) {
		prepare!Args("", sql);
		bind("", "", forward!args);
		flush();
		eatStatuses(IMT.BindComplete, true);
		describe();
		sendExecute();
		return QueryResult!T(this, FormatCode.Binary);
	}

	/++ Execute a parameterized SQL statement and return affected-row count. +/
	ulong exec(Args...)(in char[] sql, Args args) {
		prepare!Args("", sql);
		bind("", "", forward!args);
		flush();
		eatStatuses(IMT.BindComplete, true);
		sendExecute();
		eatStatuses();
		return affected;
	}

	/++ Prepare a server-side statement with the given SQL and type vector. +/
	void prepare(Args...)(in char[] statement, in char[] sql)@trusted
	if (Args.length <= short.max) {
		if (statement.length > 255)
			throw new PgSQLException("statement name is too long");
		ensureConnected();

		auto len = 5 +
			statement.length + 1 +
			sql.length + 1 +
			2 +
			Args.length * 4;
		mixin Output!(len, OMT.Parse);
		op.put(statement);
		op.put(sql);
		op.put!short(Args.length);
		foreach (T; Args)
			op.put(PgTypeof!T);
		socket.write(op.data);
	}

	/++ Bind arguments to a prepared statement or portal and stage execution params. +/
	void bind(Args...)(in char[] portal, in char[] statement, Args args) @trusted
	if (Args.length <= short.max) {
		auto len = 5 +
			portal.length + 1 +
			statement.length + 1 +
			2 + 4 +
			4 * Args.length +
			4;
		foreach (i, arg; args) {
			enum PT = PgTypeof!(Args[i]);
			static assert(PT != PgType.UNKNOWN, "Unrecognized type " ~ Args[i].stringof);
			static if (isArray!(Args[i])) {
				len += arg.length;
			} else {
				len += PT == PgType.TIMESTAMPTZ ? 4 : arg.sizeof;
			}
		}
		if (len >= int.max)
			throw new PgSQLException("bind message is too long");
		mixin Output!(len, OMT.Bind);
		op.put(portal);
		op.put(statement);
		op.put!short(1);
		op.put(FormatCode.Binary);
		op.put!short(Args.length);
		foreach (i, arg; args) {
			enum PT = PgTypeof!(Args[i]);
			static if (PT == PgType.NULL)
				op.put(-1);
			else static if (isArray!(Args[i])) {
				op.put(cast(int)arg.length);
				op.put(cast(ubyte[])arg);
			} else {
				enum int size = PT == PgType.TIMESTAMPTZ ? 4 : arg.sizeof;
				op.put(size);
				op.put(arg);
			}
		}
		op.put!short(1);
		op.put(FormatCode.Binary);
		socket.write(op.data);
	}

	/++ Describe a statement or portal before execution. +/
	void describe(in char[] name = "", DescribeType type = DescribeType.Statement) @trusted {
		auto len = 5 + 1 + name.length + 1;
		mixin Output!(len, OMT.Describe);
		op.put(type);
		op.put(name);
		socket.write(op.data);
	}

	/++ Flush buffered protocol messages without waiting for responses. +/
	void flush() {
		enum ubyte[5] buf = [OMT.Flush, 0, 0, 0, 4];
		socket.write(buf);
	}

	/++ Send a Sync message to request protocol synchronization. +/
	void sync() {
		enum ubyte[5] buf = [OMT.Sync, 0, 0, 0, 4];
		socket.write(buf);
	}

	/++ Execute a prepared portal and wait for completion, optionally with row limit. +/
	ulong executePortal(string portal = "", int rowLimit = 0) {
		sendExecute(portal, rowLimit);
		eatStatuses();
		return affected;
	}

	/++ Send a backend cancellation request for the current process session. +/
	void cancel(uint processId, uint cancellationKey) @trusted {
		ubyte[16] buf = [16, 0, 0, 0, 4, 210, 22, 46, 0, 0, 0, 0, 0, 0, 0, 0];
		*cast(uint*)&buf[8] = native(processId);
		*cast(uint*)&buf[12] = native(cancellationKey);
		socket.write(buf);
	}

	/++ Request close of a named statement/portal and consume CloseComplete. +/
	void close(DescribeType type, in char[] name = "") @trusted {
		auto len = 5 + 1 + name.length + 1;
		mixin Output!(len, OMT.Close);
		op.put(type);
		op.put(name);
		socket.write(op.data);
		flush();
		eatStatuses(IMT.CloseComplete);
	}

	/++ Start a transaction (`BEGIN`) and return transaction state. +/
	bool begin() {
		if (inTransaction)
			throw new PgSQLErrorException(
				"PgSQL doesn't support nested transactions" ~
					"- commit or rollback before starting a new transaction");

		query("begin");
		return inTransaction;
	}

	/++ Commit current transaction and clear transaction state. +/
	bool commit() {
		if (!inTransaction)
			throw new PgSQLErrorException("No active transaction");

		query("commit");
		return !inTransaction;
	}

	/++ Roll back current transaction and clear transaction state. +/
	bool rollback() {
		if (!inTransaction)
			throw new PgSQLErrorException("No active transaction");

		query("rollback");
		return !inTransaction;
	}

	/++ Close the connection and optionally send protocol terminate message. +/
	void close(bool sendTerminate = true) nothrow {
		scope (exit) {
			socket.close();
			socket = null;
		}
		enum ubyte[5] terminateMsg = [OMT.Terminate, 0, 0, 0, 4];
		if (sendTerminate)
			try
				socket.write(terminateMsg);
			catch (Exception) {
			}
	}

	/++ Re-initialize connection state for reuse from the pool. +/
	void reuse() {
		onDisconnect = null;
		ensureConnected();

		if (inTransaction)
			rollback();
	}

package(database):

	/++ Indicates whether the connection is currently leased by pool. +/
	bool busy, pooled;
	/++ Timestamp when the connection was released back to the pool. +/
	DateTime releaseTime;

private:
	/++ Close the transport and invoke disconnect callback if configured. +/
	void disconnect() {
		close(false);
		if (onDisconnect)
			onDisconnect();
	}

	/++ Open socket, send startup packet, and complete authentication exchange. +/
	void connect() @trusted {
		socket = new Socket(settings_.host, settings_.port);

		auto len = 5 + 4 +
			"user".length + 1 + settings_.user.length + 1 +
			(settings_.db.length ? "database".length + 1 + settings_.db.length + 1 : 0);
		mixin Output!len;
		alias startup = op;
		startup.put(0x00030000u);
		startup.put("user");
		startup.put(settings_.user);
		if (settings_.db.length) {
			startup.put("database");
			startup.put(settings_.db);
		}
		startup.put!ubyte(0);
		socket.write(startup.data);

		if (eatAuth())
			eatAuth();
		eatBackendKeyData(eatStatuses(IMT.BackendKeyData));
		eatStatuses();
	}

	/++ Build and send an Execute message for a prepared portal. +/
	void sendExecute(string portal = "", int rowLimit = 0) @trusted {
		auto len = 5 + portal.length + 1 + 4;
		mixin Output!(len, OMT.Execute);
		op.put(portal);
		op.put(rowLimit);
		socket.write(op.data);
		sync();
	}

	/++ Open a new connection when the current socket is not connected. +/
	void ensureConnected() {
		if (!connected)
			connect();
	}

	/++ Read a length-prefixed message with a known message-type control byte. +/
	InputPacket retrieve(ubyte control) @trusted {
		scope (failure)
			disconnect();

		uint[1] header = void;
		socket.read(header);

		auto len = native(header[0]) - 4;
		buf.length = len;
		socket.read(buf);

		if (buf.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(control, buf);
	}

	/++ Read the next message packet from the socket using its protocol header. +/
	package InputPacket retrieve() @trusted {
		scope (failure)
			disconnect();

		ubyte[5] header = void;
		socket.read(header);

		uint len = native(*cast(uint*)&header[1]) - 4;
		buf.length = len;
		socket.read(buf);

		if (buf.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(header[0], buf);
	}

	/++ Consume and process all server messages until `ReadyForQuery`. +/
	package void eatStatuses() @trusted {
		InputPacket packet = void;
		do
			packet = retrieve();
		while (eatStatus(packet) != IMT.ReadyForQuery);
	}

	/++ Consume messages until a specific message type is seen (or ReadyForQuery). +/
	package auto eatStatuses(IMT type, bool syncOnError = false) @trusted {
		InputPacket packet = void;
		do {
			packet = retrieve();
			if (packet.type == type)
				break;
		}
		while (eatStatus(packet, syncOnError) != IMT.ReadyForQuery);
		return packet;
	}

	/++ Process one authentication exchange message, including password auth flow. +/
	bool eatAuth() @trusted {
		import std.algorithm : max;

		scope (failure)
			disconnect();

		auto packet = retrieve();
		auto type = cast(IMT)packet.type;
		switch (type) with (IMT) {
		case Authentication:
			auto auth = packet.eat!uint;
			version (NoMD5Auth)
				auto len = 5 + settings_.pwd.length + 1;
			else
				auto len = 5 + max(settings_.pwd.length, 3 + 32) + 1; // 3 for md5 and 32 is hash size
			mixin Output!(len, OMT.PasswordMessage);
			switch (auth) {
			case 0:
				return false;
			case 3:
				op.put(settings_.pwd);
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
					op.put("md5".representation);
					op.put(MD5toHex(MD5toHex(settings_.pwd, settings_.user), salt));
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
				throw new PgSQLProtocolException(text("Unsupported authentication method: ", auth));
			}

			socket.write(op.data);
			break;
		case NoticeResponse:
			eatNoticeResponse(packet);
			break;
		case ErrorResponse:
			eatNoticeResponse(packet);
			throwErr();
		default:
			throw new PgSQLProtocolException(text("Unexpected message: ", type));
		}

		return true;
	}

	/++ Read and cache server parameter status key/value pairs. +/
	void eatParameterStatus(ref InputPacket packet)
	in (packet.type == IMT.ParameterStatus)
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

	/++ Cache backend process id and cancellation key. +/
	void eatBackendKeyData(InputPacket packet)
	in (packet.type == IMT.BackendKeyData) {
		server.processId = packet.eat!uint;
		server.cancellationKey = packet.eat!uint;
	}

	/++ Decode a NoticeResponse or ErrorResponse into `Notice` and store it. +/
	void eatNoticeResponse(ref InputPacket packet)
	in (packet.type == IMT.NoticeResponse || packet.type == IMT
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

	/++ Decode an asynchronous notification into `notifications_`. +/
	void eatNotification(ref InputPacket packet)
	in (packet.type == IMT.NotificationResponse) {
		notifications_ ~= Notification(packet.eat!int, packet.eatz(), packet.eatz());
	}

	/++ Parse command completion tag and update status counters. +/
	void eatCommandComplete(ref InputPacket packet)
	in (packet.type == IMT.CommandComplete) {
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
			status_.insertId = tag.parse!ulong;
			status_.affected = tag.parse!ulong(1);
			break;
		case hashOf("SELECT"), hashOf("DELETE"), hashOf("UPDATE"),
			hashOf("MOVE"), hashOf("FETCH"), hashOf("COPY"):
			status_.insertId = 0;
			status_.affected = tag.parse!ulong;
			break;
		case hashOf("CREATE"), hashOf("DROP"):
			status_.insertId = 0;
			status_.affected = 0;
			break;
		default:
		}
	}

	/++ Handle one server status packet and dispatch to specific state updates. +/
	auto eatStatus(ref InputPacket packet, bool syncOnError = false) {
		IMT type = cast(IMT)packet.type;
		switch (type) with (IMT) {
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
		case EmptyQueryResponse, NoData, ParameterDescription,
			ParseComplete, BindComplete, PortalSuspended:
			break;
		default:
			throw new PgSQLProtocolException(text("Unexpected message: ", type));
		}
		return type;
	}

	/++ Raise the latest server error as `PgSQLErrorException`. +/
	noreturn throwErr() {
		foreach (ref notice; notices_) switch (notice.severity) with (Notice.Severity) {
		case PANIC, ERROR, FATAL:
			throw new PgSQLErrorException(notice.message);
		default:
		}

		throw new PgSQLErrorException(notices_.front.message);
	}

	ubyte[] buf;

	OnDisconnectCallback onDisconnect;
	Status status_;
	Settings settings_;
	ServerInfo server;
	Notice[] notices_;
	Notification[] notifications_;
}

module database.postgresql.connection;

import std.algorithm;
import std.array;
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

struct Status
{
	bool ready;
	TransactionStatus transaction = TransactionStatus.Idle;

	ulong affected;
	ulong lastInsertId;
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

class Connection
{
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

	void query(Args...)(string sql, Args args)
	{
		//scope(failure) close();

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

	void set(T)(const(char)[] variable, T value)
	{
		query("set session ?=?", PgSQLFragment(variable), value);
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

		bool inTransaction() const {
			return connected && status_.transaction == TransactionStatus.Inside;
		}

	@property ulong lastInsertId() const
	{
		return status_.lastInsertId;
	}

		ulong affected() const nothrow @nogc { return status_.affected; }

		bool connected() const { return socket.connected; }

	void close() nothrow @nogc { socket.close(); }

	void reuse()
	{
		ensureConnected();

		if (inTransaction)
			rollback;
	}

package(database):

	bool busy, pooled;
	DateTime releaseTime;

private:

	void connect()
	{
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

		socket.write(startup.get());

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

		socket.write(cmd.get());
	}

	void ensureConnected() {
		if (!socket.connected)
			connect();
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
		//scope(failure) close();

		ubyte[4] header = void;
		socket.read(header);

		auto len = native!uint(cast(uint*)header.ptr) - 4;
		in_.length = len;
		socket.read(in_);

		if (in_.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(control, in_);
	}

	InputPacket retrieve()
	{
		//scope(failure) close();

		ubyte[5] header = void;
		socket.read(header);

		auto len = native!uint(cast(uint*)&header[1]) - 4;
		in_.length = len;
		socket.read(in_);

		if (in_.length != len)
			throw new PgSQLConnectionException("Wrong number of bytes read");

		return InputPacket(header[0], in_);
	}

	bool eatAuth(InputPacket packet)
	{
		//scope(failure) close();

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
						throw new PgSQLProtocolException("Unsupported authentication method: %s".format(auth));
				}

				reply.finalize();
				socket.write(reply.get());
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
					//writeln("  notice: ", cast(char)field, ' ', value);
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
			status_.lastInsertId = tag.parse!ulong;
			status_.affected = tag.parse!ulong(1);
				break;
		case hashOf("SELECT"), hashOf("DELETE"), hashOf("UPDATE"),
			hashOf("MOVE"), hashOf("FETCH"), hashOf("COPY"):
			status_.lastInsertId = 0;
			status_.affected = tag == "" ? 0 : tag.parse!ulong;
				break;
		case hashOf("CREATE"), hashOf("DROP"):
			status_.lastInsertId = 0;
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
		def.name = packet.eatz().idup;

		packet.skip(6);

		def.type = cast(PgType)packet.eat!uint;
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
		assert(row.values.length == header.length);

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
					assert(0);
				}
			}
			else
				row.get_(i) = PgSQLValue(null);
		}
		assert(packet.empty);
	}

	void resultSetText(RowHandler)(InputPacket packet, RowHandler handler)
	{
		auto columns = cast(size_t)packet.eat!ushort;

		columnDefs(columns, header, packet);
		row_.header(header);

		size_t index;
		while (true)
		{
			auto row = retrieve();
			if (isStatus(row))
			{
				eatStatuses(row);
				break;
			}

			resultSetRowText(row, header, row_);
			if (!callHandler(handler, index++, header, row_))
			{
				discardUntilStatus();
				break;
			}
		}
	}

	void discardAll(InputPacket packet)
	{
		auto columns = cast(size_t)packet.eatLenEnc();
		columnDefs(columns, header, packet);

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

	auto prepareSQL(Args...)(string sql, Args args)
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
					estimated += 4 + arg.peek!(const(char)[]).length;
				}
			} else static if (isArray!(typeof(arg)) && !isSomeString!(typeof(arg))) {
				argCount += arg.length;
				estimated += arg.length * 6;
			}
			else static if (isSomeString!(typeof(arg)) || is(Unqual!(typeof(arg)) == PgSQLRawString) || is(Unqual!(typeof(arg)) == PgSQLFragment) || is(Unqual!(typeof(arg)) == ubyte[]))
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

		alias AppendFunc = bool function(ref Appender!(char[]), ref string sql, ref size_t, const(void)*) @safe pure nothrow;
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
			throw new PgSQLErrorException("Wrong number of parameters for query. Got %d but expected %d.".format(argCount, indexArg));
		}

		return sql_[];
	}

	Socket socket;
	PgSQLHeader header;
	PgSQLRow row_;
	char[] schema_;
	ubyte[] in_, out_;
	ubyte seq_;
	Appender!(char[]) sql_;

	Status status_;
	Settings settings_;
	ServerInfo server;

	Notice[] notices_;
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

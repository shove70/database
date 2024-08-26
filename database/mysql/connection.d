module database.mysql.connection;

import database.mysql.exception;
import database.mysql.packet;
import database.mysql.protocol;
import database.mysql.type;
import database.mysql.row;
import database.mysql.appender;
import database.util;
import std.algorithm;
import std.array;
import std.conv : to;
import std.string;
import std.traits;
import std.uni : sicmp;
import std.utf : decode, UseReplacementDchar;
import std.datetime;

alias Socket = DBSocket!MySQLConnectionException;

enum DefaultClientCaps = CF.CLIENT_LONG_PASSWORD | CF.CLIENT_LONG_FLAG | CF.CLIENT_CONNECT_WITH_DB | CF
		.CLIENT_PROTOCOL_41 | CF.CLIENT_SECURE_CONNECTION | CF.CLIENT_SESSION_TRACK;

struct Status {
	ulong affected;
	ulong matched;
	ulong changed;
	ulong lastInsertId;
	ushort flags;
	ushort error;
	ushort warnings;
}

private:

alias CF = CapabilityFlags;

struct ConnectionSettings {
	CapabilityFlags caps = DefaultClientCaps;

	const(char)[] host;
	const(char)[] user;
	const(char)[] pwd;
	const(char)[] db;
	ushort port = 3306;
}

struct ServerInfo {
	const(char)[] versionStr;
	ubyte protocol;
	ubyte charSet;
	ushort status;
	uint connection;
	uint caps;
}

struct PreparedStatement {
	uint id;
	uint params;
}

public:

class Connection {
	this(const(char)[] host, const(char)[] user, const(char)[] pwd, const(char)[] db, ushort port = 3306, CapabilityFlags caps = DefaultClientCaps) {
		settings_.host = host;
		settings_.user = user;
		settings_.pwd = pwd;
		settings_.db = db;
		settings_.port = port;
		settings_.caps = caps | CapabilityFlags.CLIENT_LONG_PASSWORD | CapabilityFlags
			.CLIENT_PROTOCOL_41;

		connect();
	}

	void use(in char[] db) {
		send(Commands.COM_INIT_DB, db);
		eatStatus(retrieve());

		if ((caps_ & CapabilityFlags.CLIENT_SESSION_TRACK) == 0) {
			schema_.length = db.length;
			schema_[] = db[];
		}
	}

	void ping() {
		send(Commands.COM_PING);
		eatStatus(retrieve());
	}

	void refresh() {
		send(Commands.COM_REFRESH);
		eatStatus(retrieve());
	}

	void reset() {
		send(Commands.COM_RESET_CONNECTION);
		eatStatus(retrieve());
	}

	const(char)[] statistics() {
		send(Commands.COM_STATISTICS);

		auto answer = retrieve();
		return answer.eat!(const(char)[])(answer.remaining);
	}

	const(char)[] schema() const
		=> schema_;

	auto prepare(const(char)[] sql) {
		send(Commands.COM_STMT_PREPARE, sql);

		auto answer = retrieve();

		if (answer.peek!ubyte != StatusPackets.OK_Packet)
			eatStatus(answer);

		answer.expect!ubyte(0);

		auto id = answer.eat!uint;
		auto columns = answer.eat!ushort;
		auto params = answer.eat!ushort;
		answer.expect!ubyte(0);

		auto warnings = answer.eat!ushort;

		if (params) {
			foreach (i; 0 .. params)
				skipColumnDef(retrieve(), Commands.COM_STMT_PREPARE);

			eatEOF(retrieve());
		}

		if (columns) {
			foreach (i; 0 .. columns)
				skipColumnDef(retrieve(), Commands.COM_STMT_PREPARE);

			eatEOF(retrieve());
		}

		return PreparedStatement(id, params);
	}

	alias executeNoPrepare = query;

	void query(Args...)(const(char)[] sql, Args args) {
		//scope(failure) close();

		static if (args.length == 0) {
			enum shouldDiscard = true;
		} else {
			enum shouldDiscard = !isCallable!(args[$ - 1]);
		}

		enum argCount = shouldDiscard ? args.length : (args.length - 1);

		static if (argCount) {
			auto querySQL = prepareSQL(sql, args[0 .. argCount]);
		} else {
			auto querySQL = sql;
		}

		send(Commands.COM_QUERY, querySQL);

		auto answer = retrieve();
		if (isStatus(answer)) {
			eatStatus(answer);
		} else {
			static if (!shouldDiscard) {
				resultSetText(answer, Commands.COM_QUERY, args[args.length - 1]);
			} else {
				discardAll(answer, Commands.COM_QUERY);
			}
		}
	}

	void exec(Args...)(const(char)[] sql, Args args) {
		//scope(failure) close();

		PreparedStatement stmt;
		if (sql in clientPreparedCaches_) {
			stmt = clientPreparedCaches_[sql];
		} else {
			stmt = prepare(sql);
			clientPreparedCaches_[sql] = stmt;
		}

		exec(stmt, args);
		// closePreparedStatement(stmt);
	}

	void set(T)(const(char)[] variable, T value) {
		query("set session ?=?", MySQLFragment(variable), value);
	}

	const(char)[] get(const(char)[] variable) {
		const(char)[] result;
		query("show session variables like ?", variable, (MySQLRow row) {
			result = row[1].peek!(const(char)[]).dup;
		});

		return result;
	}

	void startTransaction() {
		if (inTransaction) {
			throw new MySQLErrorException(
				"MySQL does not support nested transactions - commit or rollback before starting a new transaction");
		}

		query("start transaction");

		assert(inTransaction);
	}

	void commit() {
		if (!inTransaction) {
			throw new MySQLErrorException("No active transaction");
		}

		query("commit");

		assert(!inTransaction);
	}

	void rollback() {
		if (!connected)
			return;

		if ((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0) {
			throw new MySQLErrorException("No active transaction");
		}

		query("rollback");

		assert(!inTransaction);
	}

	@property bool inTransaction() const => connected && (
		status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS);

	void exec(Args...)(PreparedStatement stmt, Args args) {
		//scope(failure) close();

		ensureConnected();

		seq_ = 0;
		auto packet = OutputPacket(&out_);
		packet.put!ubyte(Commands.COM_STMT_EXECUTE);
		packet.put!uint(stmt.id);
		packet.put!ubyte(Cursors.CURSOR_TYPE_READ_ONLY);
		packet.put!uint(1);

		static if (args.length == 0) {
			enum shouldDiscard = true;
		} else {
			enum shouldDiscard = !isCallable!(args[$ - 1]);
		}

		enum argCount = shouldDiscard ? args.length : args.length - 1;

		if (!argCount && stmt.params) {
			throw new MySQLErrorException(format("Wrong number of parameters for query. Got 0 but expected %d.", stmt
					.params));
		}

		static if (argCount) {
			enum NullsCapacity = 128; // must be power of 2
			ubyte[NullsCapacity >> 3] nulls;
			size_t bitsOut;
			size_t indexArg;

			foreach (i, arg; args[0 .. argCount]) {
				const auto index = (indexArg >> 3) & (NullsCapacity - 1);
				const auto bit = indexArg & 7;

				static if (is(typeof(arg) : typeof(null))) {
					nulls[index] = nulls[index] | (1 << bit);
					++indexArg;
				} else static if (is(Unqual!(typeof(arg)) == MySQLValue)) {
					if (arg.isNull) {
						nulls[index] = nulls[index] | (1 << bit);
					}
					++indexArg;
				} else static if (isArray!(typeof(arg)) && !isSomeString!(typeof(arg))) {
					indexArg += arg.length;
				} else {
					++indexArg;
				}

				auto finishing = (i == argCount - 1);
				auto remaining = indexArg - bitsOut;

				if (finishing || (remaining >= NullsCapacity)) {
					while (remaining) {
						auto bits = min(remaining, NullsCapacity);

						packet.put(nulls[0 .. (bits + 7) >> 3]);
						bitsOut += bits;
						nulls[] = 0;

						remaining = (indexArg - bitsOut);
						if (!remaining || (!finishing && (remaining < NullsCapacity))) {
							break;
						}
					}
				}
			}
			packet.put!ubyte(1);

			if (indexArg != stmt.params) {
				throw new MySQLErrorException(format("Wrong number of parameters for query. Got %d but expected %d.", indexArg, stmt
						.params));
			}

			foreach (arg; args[0 .. argCount]) {
				static if (is(typeof(arg) == enum)) {
					putValueType(packet, cast(OriginalType!(Unqual!(typeof(arg))))arg);
				} else {
					putValueType(packet, arg);
				}
			}

			foreach (arg; args[0 .. argCount]) {
				static if (is(typeof(arg) == enum)) {
					putValue(packet, cast(OriginalType!(Unqual!(typeof(arg))))arg);
				} else {
					putValue(packet, arg);
				}
			}
		}

		packet.finalize(seq_);
		++seq_;

		socket.write(packet.get());

		auto answer = retrieve();
		if (isStatus(answer)) {
			eatStatus(answer);
		} else {
			static if (!shouldDiscard) {
				resultSet(answer, stmt.id, Commands.COM_STMT_EXECUTE, args[$ - 1]);
			} else {
				discardAll(answer, Commands.COM_STMT_EXECUTE);
			}
		}
	}

	void closePreparedStatement(PreparedStatement stmt) {
		uint[1] data = [stmt.id];
		send(Commands.COM_STMT_CLOSE, data);
	}

	@property ulong lastInsertId() const => status_.lastInsertId;

	@property ulong affected() const => cast(size_t)status_.affected;

	@property ulong matched() const => cast(size_t)status_.matched;

	@property ulong changed() const => cast(size_t)status_.changed;

	@property size_t warnings() const => status_.warnings;

	@property size_t error() const => status_.error;

	@property const(char)[] status() const => info_;

	@property bool connected() const => socket && socket.isAlive;

	void close() nothrow {
		clearClientPreparedCache();
		socket.close();
		socket = null;
	}

	void reuse() {
		ensureConnected();

		if (inTransaction) {
			rollback;
		}

		if (settings_.db.length && (settings_.db != schema_)) {
			use(settings_.db);
		}
	}

	@property void trace(bool enable) {
		trace_ = enable;
	}

	@property bool trace() => trace_;

package(database):

	bool pooled;
	DateTime releaseTime;

	@property bool busy() const => busy_;

	@property void busy(bool value) {
		busy_ = value;

		if (!value)
			clearClientPreparedCache();
	}

private:

	void connect() {
		socket = new Socket(settings_.host, settings_.port);
		seq_ = 0;
		eatHandshake(retrieve());
		clearClientPreparedCache();
	}

	void send(T)(Commands cmd, T[] data) {
		send(cmd, cast(ubyte*)data.ptr, data.length * T.sizeof);
	}

	void send(Commands cmd, ubyte* data = null, size_t length = 0) {
		ensureConnected();

		seq_ = 0;
		auto header = OutputPacket(&out_);
		header.put!ubyte(cmd);
		header.finalize(seq_, length);
		++seq_;

		socket.write(header.get());
		if (length) {
			socket.write(data[0 .. length]);
		}
	}

	void ensureConnected() {
		if (!connected) {
			connect();
		}
	}

	void clearClientPreparedCache() nothrow {
		if (clientPreparedCaches_.length == 0)
			return;
		try
			foreach (p; clientPreparedCaches_) {
				closePreparedStatement(p);
			} catch (Exception) {
		}

		clientPreparedCaches_.clear();
	}

	bool isStatus(InputPacket packet) {
		auto id = packet.peek!ubyte;

		switch (id) {
		case StatusPackets.ERR_Packet,
			StatusPackets.OK_Packet:
			return true;
		default:
			return false;
		}
	}

	void check(InputPacket packet, bool smallError = false) {
		auto id = packet.peek!ubyte;

		switch (id) {
		case StatusPackets.ERR_Packet,
			StatusPackets.OK_Packet:
			eatStatus(packet, smallError);
			break;
		default:
			break;
		}
	}

	InputPacket retrieve() {
		//scope(failure) close();

		ubyte[4] header;
		socket.read(header);

		auto len = header[0] | (header[1] << 8) | (header[2] << 16);
		auto seq = header[3];

		if (seq != seq_) {
			throw new MySQLConnectionException("Out of order packet received");
		}

		++seq_;

		in_.length = len;
		socket.read(in_);

		if (in_.length != len) {
			throw new MySQLConnectionException("Wrong number of bytes read");
		}

		return InputPacket(&in_);
	}

	void eatHandshake(InputPacket packet) {
		//scope(failure) close();

		check(packet, true);

		server_.protocol = packet.eat!ubyte;
		server_.versionStr = packet.eat!(const(char)[])(packet.countUntil(0, true)).dup;
		packet.skip(1);

		server_.connection = packet.eat!uint;

		const auto authLengthStart = 8;
		size_t authLength = authLengthStart;

		ubyte[256] auth;
		auth[0 .. authLength] = packet.eat!(ubyte[])(authLength);

		packet.expect!ubyte(0);

		server_.caps = packet.eat!ushort;

		if (!packet.empty) {
			server_.charSet = packet.eat!ubyte;
			server_.status = packet.eat!ushort;
			server_.caps |= packet.eat!ushort << 16;
			server_.caps |= CapabilityFlags.CLIENT_LONG_PASSWORD;

			if ((server_.caps & CapabilityFlags.CLIENT_PROTOCOL_41) == 0) {
				throw new MySQLProtocolException("Server doesn't support protocol v4.1");
			}

			if (server_.caps & CapabilityFlags.CLIENT_SECURE_CONNECTION) {
				packet.skip(1);
			} else {
				packet.expect!ubyte(0);
			}

			packet.skip(10);

			authLength += packet.countUntil(0, true);
			if (authLength > auth.length) {
				throw new MySQLConnectionException("Bad packet format");
			}

			auth[authLengthStart .. authLength] = packet.eat!(ubyte[])(
				authLength - authLengthStart);

			packet.expect!ubyte(0);
		}

		caps_ = cast(CapabilityFlags)(settings_.caps & server_.caps);

		ubyte[20] token;
		{
			import std.digest.sha : sha1Of;

			auto pass = sha1Of(cast(const(ubyte)[])settings_.pwd);
			token = sha1Of(auth[0 .. authLength], sha1Of(pass));

			foreach (i; 0 .. 20) {
				token[i] = token[i] ^ pass[i];
			}
		}

		auto reply = OutputPacket(&out_);

		reply.reserve(64 + settings_.user.length + settings_.pwd.length + settings_.db.length);

		reply.put!uint(caps_);
		reply.put!uint(1);
		reply.put!ubyte(45);
		reply.fill(23);

		reply.put(settings_.user);
		reply.put!ubyte(0);

		if (settings_.pwd.length) {
			if (caps_ & CapabilityFlags.CLIENT_SECURE_CONNECTION) {
				reply.put!ubyte(token.length);
				reply.put(token);
			} else {
				reply.put(token);
				reply.put!ubyte(0);
			}
		} else {
			reply.put!ubyte(0);
		}

		if ((settings_.db.length || schema_.length) && (
				caps_ & CapabilityFlags.CLIENT_CONNECT_WITH_DB)) {
			if (schema_.length) {
				reply.put(schema_);
			} else {
				reply.put(settings_.db);

				schema_.length = settings_.db.length;
				schema_[] = settings_.db[];
			}
		}

		reply.put!ubyte(0);

		reply.finalize(seq_);
		++seq_;

		socket.write(reply.get());

		eatStatus(retrieve());
	}

	void eatStatus(InputPacket packet, bool smallError = false) {
		auto id = packet.eat!ubyte;

		switch (id) {
		case StatusPackets.OK_Packet:
			status_.matched = 0;
			status_.changed = 0;
			status_.affected = packet.eatLenEnc();
			status_.lastInsertId = packet.eatLenEnc();
			status_.flags = packet.eat!ushort;
			if (caps_ & CapabilityFlags.CLIENT_PROTOCOL_41) {
				status_.warnings = packet.eat!ushort;
			}
			status_.error = 0;
			info([]);

			if (caps_ & CapabilityFlags.CLIENT_SESSION_TRACK) {
				if (!packet.empty) {
					info(packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc()));

					if (status_.flags & StatusFlags.SERVER_SESSION_STATE_CHANGED) {
						packet.skipLenEnc();
						while (!packet.empty()) {
							final switch (packet.eat!ubyte()) with (SessionStateType) {
							case SESSION_TRACK_SCHEMA:
								packet.skipLenEnc();
								schema_.length = cast(size_t)packet.eatLenEnc();
								schema_[] = packet.eat!(const(char)[])(schema_.length);
								break;
							case SESSION_TRACK_SYSTEM_VARIABLES,
								SESSION_TRACK_GTIDS,
								SESSION_TRACK_STATE_CHANGE,
								SESSION_TRACK_TRANSACTION_STATE,
								SESSION_TRACK_TRANSACTION_CHARACTERISTICS:
								packet.skip(cast(size_t)packet.eatLenEnc());
								break;
							}
						}
					}
				}
			} else {
				info(packet.eat!(const(char)[])(packet.remaining));
			}

			import std.regex : matchFirst, regex;

			static matcher = regex(`\smatched:\s*(\d+)\s+changed:\s*(\d+)`, `i`);
			auto matches = matchFirst(info_, matcher);

			if (!matches.empty) {
				status_.matched = matches[1].to!ulong;
				status_.changed = matches[2].to!ulong;
			}

			break;
		case StatusPackets.EOF_Packet:
			status_.affected = 0;
			status_.changed = 0;
			status_.matched = 0;
			status_.error = 0;
			status_.warnings = packet.eat!ushort;
			status_.flags = packet.eat!ushort;
			info([]);

			break;
		case StatusPackets.ERR_Packet:
			status_.affected = 0;
			status_.changed = 0;
			status_.matched = 0;
			//status_.flags = 0;//[shove]
			status_.flags &= StatusFlags.SERVER_STATUS_IN_TRANS;
			status_.warnings = 0;
			status_.error = packet.eat!ushort;
			if (!smallError) {
				packet.skip(6);
			}
			info(packet.eat!(const(char)[])(packet.remaining));

			switch (status_.error) {
			case ErrorCodes.ER_DUP_ENTRY_WITH_KEY_NAME,
				ErrorCodes.ER_DUP_ENTRY:
				throw new MySQLDuplicateEntryException(info_.idup);
			case ErrorCodes.ER_DATA_TOO_LONG_FOR_COL:
				throw new MySQLDataTooLongException(info_.idup);
			case ErrorCodes.ER_DEADLOCK_FOUND:
				throw new MySQLDeadlockFoundException(info_.idup);
			case ErrorCodes.ER_TABLE_DOESNT_EXIST:
				throw new MySQLTableDoesntExistException(info_.idup);
			case ErrorCodes.ER_LOCK_WAIT_TIMEOUT:
				throw new MySQLLockWaitTimeoutException(info_.idup);
			default:
				version (development) {
					// On dev show the query together with the error message
					throw new MySQLErrorException(format("[err:%s] %s - %s", status_.error, info_, sql_
							.data));
				} else {
					throw new MySQLErrorException(format("[err:%s] %s", status_.error, info_));
				}
			}
		default:
			throw new MySQLProtocolException("Unexpected packet format");
		}
	}

	void info(const(char)[] value) {
		info_.length = value.length;
		info_[0 .. $] = value;
	}

	void skipColumnDef(InputPacket packet, Commands cmd) {
		packet.skip(cast(size_t)packet.eatLenEnc()); // catalog
		packet.skip(cast(size_t)packet.eatLenEnc()); // schema
		packet.skip(cast(size_t)packet.eatLenEnc()); // table
		packet.skip(cast(size_t)packet.eatLenEnc()); // original_table
		packet.skip(cast(size_t)packet.eatLenEnc()); // name
		packet.skip(cast(size_t)packet.eatLenEnc()); // original_name
		packet.skipLenEnc(); // next_length
		packet.skip(10); // 2 + 4 + 1 + 2 + 1           // charset, length, type, flags, decimals
		packet.expect!ushort(0);

		if (cmd == Commands.COM_FIELD_LIST) {
			packet.skip(cast(size_t)packet.eatLenEnc()); // default values
		}
	}

	void columnDef(InputPacket packet, Commands cmd, ref MySQLColumn def) {
		packet.skip(cast(size_t)packet.eatLenEnc()); // catalog
		packet.skip(cast(size_t)packet.eatLenEnc()); // schema
		packet.skip(cast(size_t)packet.eatLenEnc()); // table
		packet.skip(cast(size_t)packet.eatLenEnc()); // original_table
		auto len = cast(size_t)packet.eatLenEnc();
		def.name = packet.eat!(const(char)[])(len).idup;
		packet.skip(cast(size_t)packet.eatLenEnc()); // original_name
		packet.skipLenEnc(); // next_length
		packet.skip(2); // charset
		def.length = packet.eat!uint;
		def.type = cast(ColumnTypes)packet.eat!ubyte;
		def.flags = packet.eat!ushort;
		def.decimals = packet.eat!ubyte;

		packet.expect!ushort(0);

		if (cmd == Commands.COM_FIELD_LIST) {
			packet.skip(cast(size_t)packet.eatLenEnc()); // default values
		}
	}

	void columnDefs(size_t count, Commands cmd, ref MySQLColumn[] defs) {
		defs.length = count;
		foreach (i; 0 .. count) {
			columnDef(retrieve(), cmd, defs[i]);
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t, MySQLHeader, MySQLRow row)
	if ((ParameterTypeTuple!(RowHandler).length == 1) && is(
			ParameterTypeTuple!(RowHandler)[0] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(row);
			return true;
		} else {
			return handler(row); // return type must be bool
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t i, MySQLHeader, MySQLRow row)
	if ((ParameterTypeTuple!(RowHandler).length == 2) && isNumeric!(
			ParameterTypeTuple!(RowHandler)[0]) && is(
			ParameterTypeTuple!(RowHandler)[1] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(cast(ParameterTypeTuple!(RowHandler)[0])i, row);
			return true;
		} else {
			return handler(cast(ParameterTypeTuple!(RowHandler)[0])i, row); // return type must be bool
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t, MySQLHeader header, MySQLRow row)
	if ((ParameterTypeTuple!(RowHandler).length == 2) && is(
			ParameterTypeTuple!(RowHandler)[0] == MySQLHeader) && is(
			ParameterTypeTuple!(RowHandler)[1] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(header, row);
			return true;
		} else {
			return handler(header, row); // return type must be bool
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t i, MySQLHeader header, MySQLRow row)
	if ((ParameterTypeTuple!(RowHandler).length == 3) && isNumeric!(
			ParameterTypeTuple!(RowHandler)[0]) && is(ParameterTypeTuple!(RowHandler)[1] == MySQLHeader) && is(
			ParameterTypeTuple!(RowHandler)[2] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(i, header, row);
			return true;
		} else {
			return handler(i, header, row); // return type must be bool
		}
	}

	void resultSetRow(InputPacket packet, MySQLHeader header, ref MySQLRow row) {
		assert(row.values.length == header.length);

		packet.expect!ubyte(0);
		auto nulls = packet.eat!(ubyte[])((header.length + 2 + 7) >> 3);

		foreach (i, ref column; header) {
			const auto index = (i + 2) >> 3; // bit offset of 2
			const auto bit = (i + 2) & 7;

			if ((nulls[index] & (1 << bit)) == 0) {
				row.get_(i) = eatValue(packet, column);
			} else {
				auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
				row.get_(i) = MySQLValue(column.name, ColumnTypes.MYSQL_TYPE_NULL, signed, null, 0);
			}
		}
		assert(packet.empty);
	}

	void resultSet(RowHandler)(InputPacket packet, uint stmt, Commands cmd, RowHandler handler) {
		auto columns = cast(size_t)packet.eatLenEnc();
		columnDefs(columns, cmd, header);
		row_.header(header);

		auto status = retrieve();
		if (status.peek!ubyte == StatusPackets.ERR_Packet) {
			eatStatus(status);
		}

		size_t index;
		auto statusFlags = eatEOF(status);
		if (statusFlags & StatusFlags.SERVER_STATUS_CURSOR_EXISTS) {
			uint[2] data = [stmt, 4096]; // todo: make setting - rows per fetch
			while (statusFlags & (
					StatusFlags.SERVER_STATUS_CURSOR_EXISTS | StatusFlags
					.SERVER_MORE_RESULTS_EXISTS)) {
				send(Commands.COM_STMT_FETCH, data);

				auto answer = retrieve();
				if (answer.peek!ubyte == StatusPackets.ERR_Packet) {
					eatStatus(answer);
				}

				auto row = answer.empty ? retrieve() : answer;
				while (true) {
					if (row.peek!ubyte == StatusPackets.EOF_Packet) {
						statusFlags = eatEOF(row);
						break;
					}

					resultSetRow(row, header, row_);
					if (!callHandler(handler, index++, header, row_)) {
						discardUntilEOF(retrieve());
						statusFlags = 0;
						break;
					}
					row = retrieve();
				}
			}
		} else {
			while (true) {
				auto row = retrieve();
				if (row.peek!ubyte == StatusPackets.EOF_Packet) {
					eatEOF(row);
					break;
				}

				resultSetRow(row, header, row_);
				if (!callHandler(handler, index++, header, row_)) {
					discardUntilEOF(retrieve());
					break;
				}
			}
		}
	}

	void resultSetRowText(InputPacket packet, MySQLHeader header, ref MySQLRow row) {
		assert(row.values.length == header.length);

		foreach (i, ref column; header) {
			if (packet.peek!ubyte != 0xfb) {
				row.get_(i) = eatValueText(packet, column);
			} else {
				packet.skip(1);
				auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
				row.get_(i) = MySQLValue(column.name, ColumnTypes.MYSQL_TYPE_NULL, signed, null, 0);
			}
		}
		assert(packet.empty);
	}

	void resultSetText(RowHandler)(InputPacket packet, Commands cmd, RowHandler handler) {
		auto columns = cast(size_t)packet.eatLenEnc();
		columnDefs(columns, cmd, header);
		row_.header(header);

		eatEOF(retrieve());

		size_t index;
		while (true) {
			auto row = retrieve();
			if (row.peek!ubyte == StatusPackets.EOF_Packet) {
				eatEOF(row);
				break;
			} else if (row.peek!ubyte == StatusPackets.ERR_Packet) {
				eatStatus(row);
				break;
			}

			resultSetRowText(row, header, row_);
			if (!callHandler(handler, index++, header, row_)) {
				discardUntilEOF(retrieve());
				break;
			}
		}
	}

	void discardAll(InputPacket packet, Commands cmd) {
		auto columns = cast(size_t)packet.eatLenEnc();
		columnDefs(columns, cmd, header);

		auto statusFlags = eatEOF(retrieve());
		if ((statusFlags & StatusFlags.SERVER_STATUS_CURSOR_EXISTS) == 0) {
			while (true) {
				auto row = retrieve();
				if (row.peek!ubyte == StatusPackets.EOF_Packet) {
					eatEOF(row);
					break;
				}
			}
		}
	}

	void discardUntilEOF(InputPacket packet) {
		while (true) {
			if (packet.peek!ubyte == StatusPackets.EOF_Packet) {
				eatEOF(packet);
				break;
			}
			packet = retrieve();
		}
	}

	auto eatEOF(InputPacket packet) {
		auto id = packet.eat!ubyte;
		if (id != StatusPackets.EOF_Packet) {
			throw new MySQLProtocolException("Unexpected packet format");
		}

		status_.error = 0;
		status_.warnings = packet.eat!ushort();
		status_.flags = packet.eat!ushort();
		info([]);

		return status_.flags;
	}

	auto estimateArgs(Args...)(ref size_t estimated, Args args) {
		size_t argCount;

		foreach (i, arg; args) {
			static if (is(typeof(arg) : typeof(null))) {
				++argCount;
				estimated += 4;
			} else static if (is(Unqual!(typeof(arg)) == MySQLValue)) {
				++argCount;
				final switch (arg.type) with (ColumnTypes) {
				case MYSQL_TYPE_NULL:
					estimated += 4;
					break;
				case MYSQL_TYPE_TINY:
					estimated += 4;
					break;
				case MYSQL_TYPE_YEAR,
					MYSQL_TYPE_SHORT:
					estimated += 6;
					break;
				case MYSQL_TYPE_INT24,
					MYSQL_TYPE_LONG:
					estimated += 6;
					break;
				case MYSQL_TYPE_LONGLONG:
					estimated += 8;
					break;
				case MYSQL_TYPE_FLOAT:
					estimated += 8;
					break;
				case MYSQL_TYPE_DOUBLE:
					estimated += 8;
					break;
				case MYSQL_TYPE_SET,
					MYSQL_TYPE_ENUM,
					MYSQL_TYPE_VARCHAR,
					MYSQL_TYPE_VAR_STRING,
					MYSQL_TYPE_STRING,
					MYSQL_TYPE_JSON,
					MYSQL_TYPE_NEWDECIMAL,
					MYSQL_TYPE_DECIMAL,
					MYSQL_TYPE_TINY_BLOB,
					MYSQL_TYPE_MEDIUM_BLOB,
					MYSQL_TYPE_LONG_BLOB,
					MYSQL_TYPE_BLOB,
					MYSQL_TYPE_BIT,
					MYSQL_TYPE_GEOMETRY:
					estimated += 2 + arg.peek!(const(char)[]).length;
					break;
				case MYSQL_TYPE_TIME,
					MYSQL_TYPE_TIME2:
					estimated += 18;
					break;
				case MYSQL_TYPE_DATE,
					MYSQL_TYPE_NEWDATE,
					MYSQL_TYPE_DATETIME,
					MYSQL_TYPE_DATETIME2,
					MYSQL_TYPE_TIMESTAMP,
					MYSQL_TYPE_TIMESTAMP2:
					estimated += 20;
					break;
				}
			} else static if (isArray!(typeof(arg)) && !isSomeString!(typeof(arg))) {
				argCount += arg.length;
				estimated += arg.length * 6;
			} else static if (isSomeString!(typeof(arg)) || is(
					Unqual!(typeof(arg)) == MySQLRawString) || is(Unqual!(typeof(arg)) == MySQLFragment) || is(
					Unqual!(typeof(arg)) == MySQLBinary)) {
				++argCount;
				estimated += 2 + arg.length;
			} else {
				++argCount;
				estimated += 6;
			}
		}

		return argCount;
	}

	auto prepareSQL(Args...)(const(char)[] sql, Args args) {
		auto estimated = sql.length;
		auto argCount = estimateArgs(estimated, args);

		sql_.clear;
		sql_.reserve(max(8192, estimated));

		alias AppendFunc = bool function(ref Appender!(char[]), ref const(char)[] sql, ref size_t, const(
				void)*) @safe pure nothrow;
		AppendFunc[Args.length] funcs;
		const(void)*[Args.length] addrs;

		foreach (i, Arg; Args) {
			static if (is(Arg == enum)) {
				funcs[i] = () @trusted {
					return cast(AppendFunc)&appendNextValue!(OriginalType!Arg);
				}();
				addrs[i] = (ref x) @trusted { return cast(const void*)&x; }(
					cast(OriginalType!(Unqual!Arg))args[i]);
			} else {
				funcs[i] = () @trusted {
					return cast(AppendFunc)&appendNextValue!(Arg);
				}();
				addrs[i] = (ref x) @trusted { return cast(const void*)&x; }(args[i]);
			}
		}

		size_t indexArg;
		foreach (i; 0 .. Args.length) {
			if (!funcs[i](sql_, sql, indexArg, addrs[i])) {
				throw new MySQLErrorException(format("Wrong number of parameters for query. Got %d but expected %d.", argCount, indexArg));
			}
		}

		finishCopy(sql_, sql, argCount, indexArg);

		return sql_.data;
	}

	void finishCopy(ref Appender!(char[]) app, ref const(char)[] sql, size_t argCount, size_t indexArg) {
		if (copyUpToNext(sql_, sql)) {
			++indexArg;

			while (copyUpToNext(sql_, sql)) {
				++indexArg;
			}

			throw new MySQLErrorException(format("Wrong number of parameters for query. Got %d but expected %d.", argCount, indexArg));
		}
	}

	Socket socket;
	MySQLHeader header;
	MySQLRow row_;
	char[] info_;
	char[] schema_;
	ubyte[] in_;
	ubyte[] out_;
	ubyte seq_;
	Appender!(char[]) sql_;

	CapabilityFlags caps_;
	Status status_;
	ConnectionSettings settings_;
	ServerInfo server_;

	// For tracing queries
	bool trace_;

	PreparedStatement[const(char)[]] clientPreparedCaches_;

	bool busy_;
}

private auto copyUpToNext(ref Appender!(char[]) app, ref const(char)[] sql) {
	size_t offset;
	dchar quote = '\0';

	while (offset < sql.length) {
		auto ch = decode!(UseReplacementDchar.no)(sql, offset);
		switch (ch) {
		case '?':
			if (!quote) {
				app.put(sql[0 .. offset - 1]);
				sql = sql[offset .. $];
				return true;
			} else {
				goto default;
			}
		case '\'',
			'\"',
			'`':
			if (quote == ch) {
				quote = '\0';
			} else if (!quote) {
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
	app.put(sql[0 .. offset]);
	sql = sql[offset .. $];

	return false;
}

private bool appendNextValue(T)(ref Appender!(char[]) app, ref const(char)[] sql, ref size_t indexArg, const(
		void)* arg) {
	static if (isArray!T && !isSomeString!(OriginalType!T)) {
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

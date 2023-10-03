module database.postgresql.db;

import database.postgresql.connection,
database.postgresql.packet,
database.postgresql.protocol,
database.postgresql.row,
database.postgresql.type,
std.traits;
public import database.sqlbuilder;
import std.utf;

@safe:

struct PgSQLDB {
	Connection conn;
	alias conn this;

	this(Settings settings) {
		conn = new Connection(settings);
	}

	this(string host, string user, string pwd, string db, ushort port = 5432) {
		conn = new Connection(host, user, pwd, db, port);
	}

	bool create(Tables...)() if (Tables.length) {
		import std.array, std.meta;

		static if (Tables.length == 1)
			exec(SB.create!Tables);
		else {
			enum sql = [staticMap!(SB.create, Tables)].join(';');
			runSql(sql);
		}
		return true;
	}

	ulong insert(OR or = OR.None, T)(T s) if (isAggregateType!T) {
		mixin getSQLFields!(or ~ "INTO " ~ quote(SQLName!T) ~ '(',
			")VALUES(" ~ placeholders(ColumnCount!T) ~ ')', T);

		enum sql = SB(sql!colNames, State.insert);
		return exec(sql, s.tupleof);
	}

	ulong replaceInto(T)(T s) => insert!(OR.Replace, T)(s);

	auto selectAllWhere(T, string expr, Args...)(auto ref Args args) if (expr.length)
		=> this.query!T(SB.selectAllFrom!T.where(expr), args);

	T selectOneWhere(T, string expr, Args...)(auto ref Args args) if (expr.length) {
		auto q = query(SB.selectAllFrom!T.where(expr), args);
		if (q.empty)
			throw new PgSQLException("No match");
		return q.get!T;
	}

	T selectOneWhere(T, string expr, T defValue, Args...)(auto ref Args args)
	if (expr.length) {
		auto q = query(SB.selectAllFrom!T.where(expr), args);
		return q ? q.get!T : defValue;
	}

	bool hasTable(string table)
		=> !query("select 1 from pg_class where relname = $1", table).empty;

	bool hasTable(T)() if (isAggregateType!T) {
		enum sql = "select 1 from pg_class where relname = " ~ quote(SQLName!T);
		return !query(sql).empty;
	}

	long delWhere(T, string expr, Args...)(auto ref Args args) if (expr.length) {
		enum sql = SB.del!T.where(expr);
		return exec(sql, args);
	}
}

struct QueryResult(T = PgSQLRow) {
	Connection connection;
	alias connection this;
	PgSQLRow row;
	@disable this();

	this(Connection conn, FormatCode format = FormatCode.Text) {
		connection = conn;
		auto packet = eatStatuses(InputMessageType.RowDescription);
		if (packet.type == InputMessageType.ReadyForQuery) {
			row.values.length = 0;
			return;
		}
		auto columns = packet.eat!ushort;
		row.header = PgSQLHeader(columns, packet);
		foreach (ref col; row.header)
			col.format = format;
		popFront();
	}

	~this() {
		clear();
	}

	@property pure nothrow @nogc {
		bool empty() const => row.values.length == 0;

		PgSQLHeader header() => row.header;

		T opCast(T : bool)() const => !empty;
	}

	void popFront() {
		auto packet = eatStatuses(InputMessageType.DataRow);
		if (packet.type == InputMessageType.ReadyForQuery) {
			row.values.length = 0;
			return;
		}
		const rowlen = packet.eat!ushort;
		foreach (i, ref column; row.header)
			if (i < rowlen)
				row[i] = eatValue(packet, column);
			else
				row[i] = PgSQLValue(null);
		assert(packet.empty);
	}

	T front() {
		static if (is(Unqual!T == PgSQLRow))
			return row;
		else
			return get();
	}

	U get(U = T)() {
		static if (isAggregateType!U)
			return row.get!U;
		else
			return row[0].get!U;
	}

	U peek(U = T)() {
		static if (isAggregateType!U)
			return row.get!U;
		else
			return row[0].peek!U;
	}

	void clear() {
		if (!empty && !connection.ready) {
			eatStatuses();
			row.values.length = 0;
		}
	}
}

private:
alias SB = SQLBuilder;

PgSQLValue eatValue(ref InputPacket packet, in PgSQLColumn column) {
	import std.array;
	import std.conv : to;
	import std.datetime;

	auto length = packet.eat!uint;
	if (length == uint.max)
		return PgSQLValue(null);

	if (column.format == FormatCode.Binary) {
		switch (column.type) with (PgType) {
		case BOOL:
			return PgSQLValue(packet.eat!bool);
		case CHAR:
			return PgSQLValue(packet.eat!char);
		case INT2:
			return PgSQLValue(packet.eat!short);
		case INT4:
			return PgSQLValue(packet.eat!int);
		case INT8:
			return PgSQLValue(packet.eat!long);
		case REAL:
			return PgSQLValue(packet.eat!float);
		case DOUBLE:
			return PgSQLValue(packet.eat!double);
		case VARCHAR, CHARA,
			TEXT, NAME:
			return PgSQLValue(column.type, packet.eat!(char[])(length).dup);
		case BYTEA:
			return PgSQLValue(packet.eat!(ubyte[])(length).dup);
		case DATE:
			return PgSQLValue(packet.eat!Date);
		case TIME:
			return PgSQLValue(packet.eat!TimeOfDay);
		case TIMESTAMP:
			return PgSQLValue(packet.eat!DateTime);
		case TIMESTAMPTZ:
			return PgSQLValue(packet.eat!SysTime);
		default:
		}
		throw new PgSQLErrorException("Unsupported type " ~ column.type.columnTypeName);
	}
	auto svalue = packet.eat!(const(char)[])(length);
	switch (column.type) with (PgType) {
	case UNKNOWN, NULL:
		return PgSQLValue(null);
	case BOOL:
		return PgSQLValue(svalue[0] == 't');
	case CHAR:
		return PgSQLValue(svalue[0]);
	case INT2:
		return PgSQLValue(svalue.to!short);
	case INT4:
		return PgSQLValue(svalue.to!int);
	case INT8:
		return PgSQLValue(svalue.to!long);
	case REAL:
		return PgSQLValue(svalue.to!float);
	case DOUBLE:
		return PgSQLValue(svalue.to!double);

	case NUMERIC, MONEY,
		BIT, VARBIT,
		INET, CIDR, MACADDR, MACADDR8,
		UUID, JSON, XML,
		TEXT, NAME,
		VARCHAR, CHARA:
		return PgSQLValue(column.type, svalue.dup);
	case BYTEA:
		if (svalue.length >= 2)
			svalue = svalue[2 .. $];
		auto data = uninitializedArray!(ubyte[])(svalue.length >> 1);
		foreach (i; 0 .. data.length)
			data[i] = cast(ubyte)(hexDecode(svalue[i << 1]) << 4 | hexDecode(svalue[i << 1 | 1]));
		return PgSQLValue(data);
	case DATE:
		return PgSQLValue(parseDate(svalue));
	case TIME, TIMETZ:
		return PgSQLValue(parsePgSQLTime(svalue));
	case TIMESTAMP, TIMESTAMPTZ:
		return PgSQLValue(parsePgSQLTimestamp(svalue));
	default:
	}
	throw new PgSQLErrorException("Unsupported type " ~ column.type.columnTypeName);
}

uint hexDecode(char c) @nogc pure nothrow
	=> c + 9 * (c >> 6) & 15;

public unittest {
	import database.util;
	import std.stdio;

	@snakeCase struct PlaceOwner {
	@snakeCase:
		@sqlkey() uint placeID; // matches place_id
		float locationId; // matches location_id
		string ownerName; // matches owner_name
		string feedURL; // matches feed_url
	}

	auto db = PgSQLDB("127.0.0.1", "postgres", "postgres", "postgres");
	db.runSql(`CREATE TABLE IF NOT EXISTS company(
	ID INT PRIMARY KEY	NOT NULL,
	NAME		TEXT	NOT NULL,
	AGE			INT		NOT NULL,
	ADDRESS		CHAR(50),
	SALARY		REAL,
	JOIN_DATE	DATE
);`);
	assert(db.hasTable("company"));
	assert(db.query("select 42").get!int == 42);
	db.create!PlaceOwner;
	db.insert(PlaceOwner(1, 1, "foo", ""));
	db.insert(PlaceOwner(2, 1, "bar", ""));
	db.insert(PlaceOwner(3, 7.5, "baz", ""));
	auto s = db.selectOneWhere!(PlaceOwner, "owner_name=$1")("bar");
	assert(s.placeID == 2);
	assert(s.ownerName == "bar");
	uint count;
	foreach (row; db.selectAllWhere!(PlaceOwner, "location_id=$1")(1)) {
		writeln(row);
		count++;
	}
	db.exec("drop table company");
	db.exec("drop table place_owner");
	assert(count == 2);
	db.close();
}

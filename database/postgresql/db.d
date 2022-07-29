module database.postgresql.db;

// dfmt off
import
	database.postgresql.connection,
	database.postgresql.packet,
	database.postgresql.protocol,
	database.postgresql.row,
	database.postgresql.type,
	std.traits;
// dfmt on
public import database.sqlbuilder;

alias PgSQLDB = Connection;

struct QueryResult(T = PgSQLRow) {
	Connection connection;
	alias connection this;
	PgSQLRow row;
	@disable this();

@safe:
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
		bool empty() const {
			return row.values.length == 0;
		}

		PgSQLHeader header() {
			return row.header;
		}

		T opCast(T : bool)() const {
			return !empty;
		}
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
				eatValue(packet, column, row[i]);
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
		if (!empty) {
			eatStatuses();
			row.values.length = 0;
		}
	}
}

@safe private:
alias SB = SQLBuilder;

void eatValue(ref InputPacket packet, in PgSQLColumn column, ref PgSQLValue value) {
	import std.array;
	import std.conv : to;
	import std.datetime;
	static import std.uuid;

	auto length = packet.eat!uint;
	if (length == uint.max) {
		value = PgSQLValue(null);
		return;
	}
	if (column.format == FormatCode.Binary) {
		switch (column.type) with (PgType) {
		case BOOL:
			value = PgSQLValue(packet.eat!bool);
			return;
		case CHAR:
			value = PgSQLValue(packet.eat!char);
			return;
		case INT2:
			value = PgSQLValue(packet.eat!short);
			return;
		case INT4:
			value = PgSQLValue(packet.eat!int);
			return;
		case INT8:
			value = PgSQLValue(packet.eat!long);
			return;
		case REAL:
			value = PgSQLValue(packet.eat!float);
			return;
		case DOUBLE:
			value = PgSQLValue(packet.eat!double);
			return;
		case VARCHAR, CHARA:
		case TEXT, NAME:
			value = PgSQLValue(column.type, packet.eat!(char[])(length).idup);
			return;
		case BYTEA:
			value = PgSQLValue(packet.eat!(ubyte[])(length).dup);
			return;
		case DATE:
			value = PgSQLValue(packet.eat!Date);
			break;
		case TIME:
			value = PgSQLValue(packet.eat!TimeOfDay);
			break;
		case TIMESTAMP:
			value = PgSQLValue(packet.eat!DateTime);
			break;
		case TIMESTAMPTZ:
			value = PgSQLValue(packet.eat!SysTime);
			break;
		default:
			throw new PgSQLErrorException("Unsupported type " ~ column.type.columnTypeName);
		}
	}
	auto svalue = packet.eat!string(length);
	switch (column.type) with (PgType) {
	case UNKNOWN, NULL:
		value = PgSQLValue(null);
		break;
	case BOOL:
		value = PgSQLValue(svalue[0] == 't');
		break;
	case CHAR:
		value = PgSQLValue(svalue[0]);
		break;
	case INT2:
		value = PgSQLValue(svalue.to!short);
		break;
	case INT4:
		value = PgSQLValue(svalue.to!int);
		break;
	case INT8:
		value = PgSQLValue(svalue.to!long);
		break;
	case REAL:
		value = PgSQLValue(svalue.to!float);
		break;
	case DOUBLE:
		value = PgSQLValue(svalue.to!double);
		break;

	case NUMERIC:
	case MONEY:
	case BIT, VARBIT:
	case INET, CIDR, MACADDR, MACADDR8:
	case UUID, JSON, XML:
	case TEXT, NAME:
	case VARCHAR, CHARA:
		value = PgSQLValue(column.type, svalue);
		break;
	case BYTEA:
		if (svalue.length >= 2)
			svalue = svalue[2 .. $];
		auto data = uninitializedArray!(ubyte[])(svalue.length >> 1);
		foreach (i; 0 .. data.length)
			data[i] = cast(ubyte)(hexDecode(svalue[i << 1]) << 4 | hexDecode(svalue[i << 1 | 1]));
		value = PgSQLValue(data);
		break;
	case DATE:
		value = PgSQLValue(parseDate(svalue));
		break;
	case TIME, TIMETZ:
		value = PgSQLValue(parsePgSQLTime(svalue));
		break;
	case TIMESTAMP, TIMESTAMPTZ:
		value = PgSQLValue(parsePgSQLTimestamp(svalue));
		break;
	default:
		throw new PgSQLErrorException("Unsupported type " ~ column.type.columnTypeName);
	}
}

uint hexDecode(char c) @nogc pure nothrow {
	return c + 9 * (c >> 6) & 15;
}

public:
bool create(T)(PgSQLDB db) {
	db.exec(SB.create!T);
	return true;
}

ulong insert(OR or = OR.None, T)(PgSQLDB db, T s) if (isAggregateType!T) {
	mixin getSQLFields!(or ~ "INTO " ~ quote(SQLName!T) ~ '(',
		")VALUES(" ~ placeholders(ColumnCount!T) ~ ')', T);

	return db.exec(SB(sql!colNames, State.insert), s.tupleof);
}

auto selectAllWhere(T, string expr, ARGS...)(PgSQLDB db, ARGS args) if (expr.length) {
	return db.query!T(SB.selectAllFrom!T.where(expr), args);
}

T selectOneWhere(T, string expr, ARGS...)(PgSQLDB db, ARGS args) if (expr.length) {
	auto q = db.query(SB.selectAllFrom!T.where(expr), args);
	if (q.empty)
		throw new PgSQLException("No match");
	return q.get!T;
}

T selectOneWhere(T, string expr, T defValue, ARGS...)(PgSQLDB db, ARGS args)
if (expr.length) {
	auto q = db.query(SB.selectAllFrom!T.where(expr), args);
	return q ? q.get!T : defValue;
}

bool hasTable(PgSQLDB db, string table) {
	return !db.query("select 1 from pg_class where relname = $1", table).empty;
}

long delWhere(T, string expr, ARGS...)(PgSQLDB db, ARGS args) if (expr.length) {
	return db.exec(SB.del!T.where(expr), args);
}

unittest {
	import database.util;
	import std.stdio;

	@snakeCase struct PlaceOwner {
	@snakeCase:
		@sqlkey() uint placeID; // matches place_id
		uint locationId; // matches location_id
		string ownerName; // matches owner_name
		string feedURL; // matches feed_url
	}

	scope db = new PgSQLDB("127.0.0.1", "postgres", "postgres", "postgres");
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
	db.insert(PlaceOwner(3, 3, "baz", ""));
	auto s = db.selectOneWhere!(PlaceOwner, "owner_name=$1")("bar");
	assert(s.placeID == 2);
	assert(s.ownerName == "bar");
	foreach (row; db.selectAllWhere!(PlaceOwner, "location_id=$1")(1))
		writeln(row);
	db.exec("drop table place_owner");
	db.exec("drop table company");
	db.close();
}

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
	PgSQLHeader cols;
	@disable this();

	this(Connection conn) {
		connection = conn;
		auto packet = retrieve();
		if (packet.isStatus && eatStatuses(packet) == InputMessageType.ReadyForQuery) {
			row.values.length = 0;
			return;
		}
		auto columns = packet.eat!ushort;
		cols = PgSQLHeader(columns, packet);
		row.header = cols;
		popFront();
	}

	~this() {
		clear();
	}

	@property pure @safe nothrow @nogc {
		bool empty() const {
			return row.values.length == 0;
		}

		const(PgSQLHeader) header() const {
			return cols;
		}
	}

	void popFront() {
		auto packet = retrieve();
		if (packet.isStatus && eatStatuses(packet) == InputMessageType.ReadyForQuery) {
			row.values.length = 0;
			return;
		}
		assert(row.header.length == cols.length);
		assert(packet.type == InputMessageType.DataRow);
		const rowlen = packet.eat!ushort;

		foreach (i, ref column; cols)
			if (i < rowlen) {
				assert(column.format == FormatCode.Text);
				eatValueText(packet, column, row[i]);
			} else
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
		if (empty)
			return;
		for (;;) {
			auto packet = retrieve();
			if (packet.isStatus) {
				eatStatuses(packet);
				row.values.length = 0;
				break;
			}
		}
	}

	void toString(R)(ref R app) const {
		row.toString(app);
	}

	string toString() const {
		return row.toString;
	}
}

private alias SB = SQLBuilder;

private void eatValueText(ref InputPacket packet, in PgSQLColumn column, ref PgSQLValue value) {
	import std.array;
	import std.conv : to;

	auto length = packet.eat!uint;
	if (length == uint.max) {
		value = PgSQLValue(null);
		return;
	}
	auto svalue = packet.eat!string(length);
	switch(column.type) with (PgType) {
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
		value = PgSQLValue(column.type, data);
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
	}
}

private uint hexDecode(char c) @safe @nogc pure nothrow {
	return c + 9 * (c >> 6) & 15;
}

package bool isStatus(in InputPacket packet) {
	switch (packet.type) with (InputMessageType) {
	case ErrorResponse, NoticeResponse, ReadyForQuery:
	case NotificationResponse, CommandComplete:
		return true;
	default:
		return false;
	}
}

bool create(T)(PgSQLDB db) {
	db.query(SB.create!T);
	return true;
}

long insert(OR or = OR.None, T)(PgSQLDB db, T s) if (isAggregateType!T) {
	import std.array : replicate;

	enum qms = ",?".replicate(ColumnCount!T);
	mixin getSQLFields!(or ~ "INTO " ~ quote(SQLName!T) ~ '(',
		") VALUES(" ~ (qms.length ? qms[1 .. $] : qms) ~ ')', T);

	db.query(SB(sql!colNames, State.insert), s.tupleof);
	return db.affected;
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
	return q.empty ? q.get!T : defValue;
}

bool hasTable(PgSQLDB db, string table) {
	return !db.query("select 1 from pg_class where relname = ?", table).empty;
}

long delWhere(T, string expr, ARGS...)(PgSQLDB db, ARGS args) if (expr.length) {
	db.query(SB.del!T.where(expr), args);
	return db.affected;
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
	db.exec(`CREATE TABLE IF NOT EXISTS company(
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
	auto s = db.selectOneWhere!(PlaceOwner, "owner_name=?")("bar");
	assert(s.placeID == 2);
	assert(s.ownerName == "bar");
	foreach (row; db.selectAllWhere!(PlaceOwner, "location_id=?")(1))
		writeln(row);
	assert(db.exec("drop table place_owner"));
	assert(db.exec("drop table company"));
	db.close();
}

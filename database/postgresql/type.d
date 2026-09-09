module database.postgresql.type;

import core.bitop : bsr;
import database.postgresql.protocol;
import database.postgresql.packet;
import database.postgresql.row;
import std.conv : text;
import std.datetime;
import std.format : formattedWrite;
import std.traits;
public import database.util;

/++ Returns whether `T` is a value type that can be represented by `PgSQLValue`. +/
enum isValueType(T) = !is(T == struct) || is(Unqual!T == PgSQLValue) ||
	is(T : Date) || is(T : DateTime) || is(T : SysTime);

/++ Map a D type to a PostgreSQL `PgType` enum.

Supports enums, integral types, chars, strings and temporal types used by this
library. +/
template PgTypeof(T) {
	static if (is(T U == enum)) {
		static if (is(Unqual!T == PgType))
			enum PgTypeof = PgType.OID;
		else
			enum PgTypeof = PgTypeof!U;
	} else static if (is(T : typeof(null)))
		enum PgTypeof = PgType.NULL;
	else static if (isIntegral!T)
		enum PgTypeof = [PgType.INT2, PgType.INT4, PgType.INT8][T.sizeof / 4];
	else static if (isSomeString!T)
		enum PgTypeof = PgType.TEXT;
	else static if (isSomeChar!T)
		enum PgTypeof = PgType.CHAR;
	else {
		alias U = Unqual!T;
		static if (is(U == float))
			enum PgTypeof = PgType.REAL;
		else static if (is(U == double))
			enum PgTypeof = PgType.DOUBLE;
		else static if (is(U == Date))
			enum PgTypeof = PgType.DATE;
		else static if (is(U == TimeOfDay) || is(U == PgSQLTime))
			enum PgTypeof = PgType.TIME;
		else static if (is(U == DateTime) || is(U == PgSQLTimestamp))
			enum PgTypeof = PgType.TIMESTAMP;
		else static if (is(U == SysTime))
			enum PgTypeof = PgType.TIMESTAMPTZ;
		else static if (is(U : const(ubyte)[]) || is(U : ubyte[n], size_t n))
			enum PgTypeof = PgType.BYTEA;
		else
			enum PgTypeof = PgType.UNKNOWN;
	}
}

@safe pure:

/++ Runtime value container used by PostgreSQL rows and protocol codecs. +/
struct PgSQLValue {
	/++ Construct a value directly from protocol text payload and PostgreSQL type. +/
	package this(PgType type, char[] str) {
		type_ = type;
		arr = cast(ubyte[])str;
	}

	/++ Construct a SQL `NULL` value. +/
	this(typeof(null)) {
		type_ = PgType.NULL;
	}

	/++ Construct a BOOLEAN value. +/
	this(bool value) {
		type_ = PgType.BOOL;
		p = value ? 't' : 'f';
	}

	/++ Construct numeric values from supported scalar types (`T` template overload group). +/
	this(T)(T value) @trusted if (isScalarType!T && !isBoolean!T) {
		static if (isFloatingPoint!T) {
			static assert(T.sizeof <= 8, "Unsupported type: " ~ T.stringof);
			enum t = [PgType.REAL, PgType.DOUBLE][T.sizeof / 8];
		} else
			enum t = [PgType.CHAR, PgType.INT2, PgType.INT4, PgType.INT8][bsr(T.sizeof)];
		type_ = t;

		*cast(Unqual!T*)&p = value;
	}

	/++ Construct from a `Date`. +/
	this(Date value) @trusted {
		type_ = PgType.DATE;
		timestamp.date = value;
	}

	/++ Construct from `TimeOfDay` by translating to PostgreSQL `TIME`. +/
	this(TimeOfDay value) {
		this(PgSQLTime(value.hour, value.minute, value.second));
	}

	/++ Construct from `PgSQLTime` (`TIME` type). +/
	this(PgSQLTime value) @trusted {
		type_ = PgType.TIME;
		timestamp.time = value;
	}

	/++ Construct from `DateTime` as PostgreSQL `TIMESTAMP`. +/
	this(DateTime value) {
		this(PgSQLTimestamp(value.date, PgSQLTime(value.hour, value.minute, value.second)));
	}

	/++ Construct from `SysTime` as PostgreSQL `TIMESTAMPTZ`. +/
	this(in SysTime value) @trusted {
		this(cast(DateTime)value);
		type_ = PgType.TIMESTAMPTZ;
	}

	/++ Construct from a `PgSQLTimestamp` timestamp wrapper. +/
	this(in PgSQLTimestamp value) @trusted {
		type_ = PgType.TIMESTAMP;
		timestamp.date = value.date;
		timestamp.time = value.time;
	}

	/++ Construct from UTF-8 text (`VARCHAR`/`TEXT`). +/
	this(const(char)[] value) @trusted {
		type_ = PgType.VARCHAR;
		arr = cast(ubyte[])value;
	}

	/++ Construct from binary data (`BYTEA`). +/
	this(const(ubyte)[] value) @trusted {
		type_ = PgType.BYTEA;
		arr = cast(ubyte[])value;
	}

	/++ Serialize value into a generic writer with PostgreSQL SQL formatting rules. +/
	void toString(R)(ref R app) @trusted const {
		switch (type_) with (PgType) {
		case BOOL:
			app.put(*cast(bool*)&p ? "TRUE" : "FALSE");
			break;
		case CHAR:
			app.formattedWrite("%s", *cast(ubyte*)&p);
			break;
		case INT2:
			app.formattedWrite("%d", *cast(short*)&p);
			break;
		case INT4:
			app.formattedWrite("%d", *cast(int*)&p);
			break;
		case INT8:
			app.formattedWrite("%d", *cast(long*)&p);
			break;
		case REAL:
			app.formattedWrite("%g", *cast(float*)&p);
			break;
		case DOUBLE:
			app.formattedWrite("%g", *cast(double*)&p);
			break;
		case POINT, LSEG, PATH, BOX, POLYGON, LINE,
			TINTERVAL,
			CIRCLE,
			JSONB,
		BYTEA:
			app.formattedWrite("%s", arr);
			break;
		case MONEY,
			TEXT, NAME,
			BIT, VARBIT,
			NUMERIC,
			INET, CIDR, MACADDR, MACADDR8,
			UUID, JSON, XML,
			CHARA, VARCHAR:
			app.put(*cast(string*)&p);
			break;
		case DATE:
			timestamp.date.toString(app);
			break;
		case TIME, TIMETZ:
			timestamp.time.toString(app);
			break;
		case TIMESTAMP, TIMESTAMPTZ:
			timestamp.toString(app);
			break;
		default:
		}
	}

	/++ Return the SQL-friendly text form of this value. +/
	string toString() const {
		import std.array : appender;

		auto app = appender!string;
		toString(app);
		return app[];
	}

	/++ Compare two `PgSQLValue` values with type-aware conversion. +/
	bool opEquals(PgSQLValue other) const {
		if (isString && other.isString)
			return peek!string == other.peek!string;
		if (isScalar == other.isScalar) {
			if (isFloat || other.isFloat)
				return get!double == other.get!double;
			return get!long == other.get!long;
		}
		if (isTime == other.isTime)
			return get!Duration == other.get!Duration;
		if (isTimestamp == other.isTimestamp)
			return get!SysTime == other.get!SysTime;
		return isNull == other.isNull;
	}

	/++ Return `def` when NULL, otherwise convert to `T`. +/
	T get(T)(lazy T def) const => !isNull ? get!T : def;

	/++ Convert to non-enum scalar output types. +/
	// dfmt off
	T get(T)() @trusted const if (isScalarType!T && !is(T == enum)) {
		switch(type_) with (PgType) {
		case CHAR: return cast(T)*cast(char*)&p;
		case INT2: return cast(T)*cast(short*)&p;
		case INT4: return cast(T)*cast(int*)&p;
		case INT8: return cast(T)*cast(long*)&p;
		case REAL: return cast(T)*cast(float*)&p;
		case DOUBLE: return cast(T)*cast(double*)&p;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName,
			" to " ~ T.stringof));
	}
	// dfmt on

	/++ Convert to `SysTime` for date-time fields. +/
	T get(T : SysTime)() @trusted const {
		switch (type_) with (PgType) {
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toSysTime;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName,
			" to " ~ T.stringof));
	}

	/++ Convert to `DateTime`. +/
	T get(T : DateTime)() @trusted const {
		switch (type_) with (PgType) {
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toDateTime;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName,
			" to " ~ T.stringof));
	}

	/++ Convert to `TimeOfDay`. +/
	T get(T : TimeOfDay)() @trusted const {
		switch (type_) with (PgType) {
		case TIME, TIMETZ:
			return time.toTimeOfDay;
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toTimeOfDay;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName,
			" to " ~ T.stringof));
	}

	/++ Convert `TIME`/`TIMESTAMP` values to `Duration`. +/
	T get(T : Duration)() @trusted const {
		switch (type_) with (PgType) {
		case TIME, TIMETZ,
			TIMESTAMP, TIMESTAMPTZ:
			return timestamp.time.toDuration;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName,
			" to " ~ T.stringof));
	}

	/++ Convert date-like PostgreSQL fields to `Date`. +/
	T get(T)() @trusted const if (is(T : Date)) {
		switch (type_) with (PgType) {
		case DATE,
			TIMESTAMP, TIMESTAMPTZ:
			return timestamp.date;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName,
			" to " ~ T.stringof));
	}

	/++ Convert enum values via underlying stored value mapping. +/
	T get(T)() const if (is(T == enum))
		=> cast(T)get!(OriginalType!T);

	/++ Convert textual and binary types to array/static array outputs. +/
	T get(T)() const @trusted if (isArray!T && !is(T == enum)) {
		switch (type_) with (PgType) {
		case NUMERIC,
			MONEY,
			BIT, VARBIT,
			INET, CIDR, MACADDR, MACADDR8,
			UUID, JSON, XML,
			TEXT, NAME,
			VARCHAR, CHARA,
		BYTEA:
			static if (isStaticArray!T)
				return cast(T)arr[0 .. T.sizeof];
			else
				return cast(T)arr.dup;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName, " to array"));
	}

	/++ Access converted values with defaults and overload group support.

	The overload set includes:
	- `peek!T(lazy T def)` for nullable fallback
	- `peek!T()` for structs/static arrays
	- `peek!T()` for dynamic arrays
+/
	T peek(T)(lazy T def) const => !isNull ? peek!T : def;

	T peek(T)() const if (is(T == struct) || isStaticArray!T) => get!T;

	T peek(T)() @trusted const if (is(T == U[], U)) {
		switch (type_) with (PgType) {
		case NUMERIC,
			MONEY,
			BIT, VARBIT,
			INET, CIDR, MACADDR, MACADDR8,
			UUID, JSON, XML,
			TEXT, NAME,
			VARCHAR, CHARA:
			return cast(T)arr;
		default:
		}
		throw new PgSQLErrorException(text("Cannot convert ", type_.columnTypeName, " to array"));
	}

	/++ Compute a hash code derived from the underlying payload and type tag. +/
	size_t toHash() const @nogc @trusted pure nothrow
		=> *cast(size_t*)&p ^ (type_ << 24);

	@property nothrow @nogc {

		/++ Check whether the value is SQL `NULL`. +/
		bool isNull() const => type_ == PgType.NULL;

		/++ Check whether the value type is unknown. +/
		bool isUnknown() const => type_ == PgType.UNKNOWN;

		/++ Get the PostgreSQL type tag. +/
		PgType type() const => type_;

		/++ Check whether the value is string-like and serializable as text. +/
		bool isString() const {
			switch (type_) with (PgType) {
			case NUMERIC,
				MONEY,
				BIT, VARBIT,
				INET, CIDR, MACADDR, MACADDR8,
				UUID, JSON, XML,
				TEXT, NAME,
				VARCHAR, CHARA:
				return true;
			default:
			}
			return false;
		}

		/++ Check whether the value is stored as a numeric scalar. +/
		bool isScalar() const {
			switch (type_) with (PgType) {
			case BOOL, CHAR,
				INT2, INT4, INT8,
				REAL, DOUBLE:
				return true;
			default:
			}
			return false;
		}

		/++ Check whether the value is float or double. +/
		bool isFloat() const => type_ == PgType.REAL || type_ == PgType.DOUBLE;

		/++ Check whether the value is `TIME` or `TIMETZ`. +/
		bool isTime() const => type_ == PgType.TIME || type_ == PgType.TIMETZ;

		/++ Check whether the value is `DATE`. +/
		bool isDate() const => type_ == PgType.DATE;

		/++ Check whether the value is `TIMESTAMP` family. +/
		bool isTimestamp() const => type_ == PgType.TIMESTAMP || type_ == PgType.TIMESTAMPTZ;
	}

private:
	static if (size_t.sizeof > 4) {
		union {
			struct {
				uint length;
				PgType type_;
				ubyte p;
			}

			ubyte[] _arr;
			PgSQLTimestamp timestamp;
		}

		@property const(ubyte)[] arr() @trusted const {
			union Array {
				const ubyte[] arr;
				size_t length;
			}

			auto u = Array(_arr);
			u.length &= uint.max;
			return u.arr;
		}

		@property ubyte[] arr(ubyte[] arr) @trusted
		in (arr.length <= uint.max) {
			const type = type_;
			_arr = arr;
			type_ = type;
			return arr;
		}
	} else {
		PgType type_;
		union {
			ubyte p;
			ubyte[] arr;
			PgSQLTimestamp timestamp;
		}
	}
}

/++ Column metadata from PostgreSQL `RowDescription` messages. +/
struct PgSQLColumn {
	/++ Name of the field. +/
	string name;

	/++ Table OID when the field maps to a table, otherwise zero. +/
	int table;

	/++ Column attribute number for the table field, or zero when not available. +/
	short columnId;

	/++ Data type size from `pg_type.typlen`. +/
	short length;

	/++ PostgreSQL field type identifier. +/
	PgType type;

	/++ Type modifier from `pg_attribute.atttypmod`. +/
	int modifier;

	/++ Field format: `0` for text, `1` for binary. +/
	FormatCode format;
}

/++ Header wrapper around a set of PostgreSQL columns. +/
struct PgSQLHeader {
	/++ Column entries in row order. +/
	PgSQLColumn[] cols;
	alias cols this;

	/++ Read `count` column descriptors from the protocol packet. +/
	this(size_t count, ref InputPacket packet) @trusted {
		import std.array;

		cols = uninitializedArray!(PgSQLColumn[])(count);
		foreach (ref def; cols) {
			def.name = packet.eatz().idup;
			def.table = packet.eat!int;
			def.columnId = packet.eat!short;
			def.type = packet.eat!PgType;
			def.length = packet.eat!short;
			def.modifier = packet.eat!int;
			def.format = packet.eat!FormatCode;
		}
	}
}

struct PgSQLTime {
/++ Time value used by PostgreSQL `TIME` and `TIMETZ`. +/
	union {
		uint _usec;
		struct {
			version (LittleEndian) {
				private byte[3] pad;
				byte moffset;
			} else {
				byte moffset;
				private byte[3] pad;
			}
		}
	}

	ubyte hour, minute, second;
	byte hoffset;

	/++ Create a time from hour/minute/second components. +/
	this(ubyte h, ubyte m, ubyte s) {
		hour = h;
		minute = m;
		second = s;
	}

	/++ Internal constructor for decoded values including microseconds and offset. +/
	private this(uint usec, ubyte h, ubyte m, ubyte s, byte hoffset = 0) pure {
		_usec = usec;
		hour = h;
		minute = m;
		second = s;
		this.hoffset = hoffset;
	}

	/++ Microseconds within the second. +/
	@property uint usec() const => _usec & 0xFFFFFF;

	@property uint usec(uint usec)
	in (usec <= 0xFFFFFF) {
		_usec = usec | moffset << 24;
		return usec;
	}

	invariant ((_usec & 0xFFFFFF) < 1_000_000);
	invariant (hour < 24 && minute < 60 && second < 60);
	invariant (0 <= hour + hoffset && hour + hoffset < 24);
	invariant (0 <= minute + moffset && minute + moffset < 60);

	/++ Convert to duration from midnight including offset and microseconds. +/
	Duration toDuration() const => usecs((hour + hoffset) * 3600_000_000L + (
			minute + moffset) * 60_000_000L +
			second * 1_000_000L +
			usec);

	/++ Convert to `TimeOfDay` with applied offsets. +/
	TimeOfDay toTimeOfDay() const => TimeOfDay(hour + hoffset, minute + moffset, second);

	/++ Serialize the time value to a writer. +/
	void toString(W)(ref W w) const {
		w.formattedWrite("%02d:%02d:%02d", hour, minute, second);
		if (usec) {
			uint usecabv = usec;
			if (usecabv % 1000 == 0)
				usecabv /= 1000;
			if (usecabv % 100 == 0)
				usecabv /= 100;
			if (usecabv % 10 == 0)
				usecabv /= 10;
			w.formattedWrite(".%d", usecabv);
		}
		if (hoffset || moffset) {
			if (hoffset < 0 || moffset < 0) {
				w.formattedWrite("-%02d", -hoffset);
				if (moffset)
					w.formattedWrite(":%02d", -moffset);
			} else {
				w.formattedWrite("+%02d", hoffset);
				if (moffset)
					w.formattedWrite(":%02d", moffset);
			}
		}
	}
}

struct PgSQLTimestamp {
	/++ Timestamp value in PostgreSQL wire-compatible layout. +/
	Date date;
	align(size_t.sizeof) PgSQLTime time;

	/++ Convert to `SysTime`, using an explicit timezone when offsets are set. +/
	SysTime toSysTime() const {
		auto datetime = DateTime(date.year, date.month, date.day, time.hour, time.minute, time
				.second);
		if (time.hoffset || time.moffset) {
			const offset = time.hoffset.hours + time.moffset.minutes;
			return SysTime(datetime, time.usec.usecs, new immutable SimpleTimeZone(offset));
		}
		return SysTime(datetime, time.usec.usecs);
	}

	/++ Extract `TimeOfDay`. +/
	TimeOfDay toTimeOfDay() const => time.toTimeOfDay();

	/++ Convert to D `DateTime` without timezone conversion. +/
	DateTime toDateTime() const => DateTime(date, time.toTimeOfDay());

	/++ Serialize this timestamp in standard textual PostgreSQL format. +/
	void toString(R)(ref R app) const {
		app.formattedWrite("%04d-%02d-%02d ", date.year, date.month, date.day);
		time.toString(app);
	}
}

auto parseDate(ref scope const(char)[] x) {
	int year = x.parse!int(0);
	x.skip('-');
	int month = x.parse!int(0);
	x.skip('-');
	int day = x.parse!int(0);
	return Date(year, month, day);
}

/++ Parse PostgreSQL time string representation into `PgSQLTime`. +/
auto parsePgSQLTime(ref scope const(char)[] x) {
	auto hour = x.parse!uint(0);
	x.skip(':');
	auto minute = x.parse!uint(0);
	x.skip(':');
	auto second = x.parse!uint(0);
	uint usecs;

	if (x.length && x[0] == '.') {
		x.skip();
		const len = x.length;
		usecs = x.parse!uint(0);
		const d = 6 - (len - x.length);
		if (d < 0 || d > 5)
			throw new PgSQLProtocolException("Bad datetime string format");

		usecs *= 10 ^^ d;
	}

	byte hoffset, moffset;

	if (x.length) {
		auto sign = x[0] == '-' ? -1 : 1;
		x.skip();

		hoffset = cast(byte)(sign * x.parse!int(0));
		if (x.length) {
			x.skip(':');
			moffset = cast(byte)(sign * x.parse!int(0));
		}
	}

	auto res = PgSQLTime(usecs, cast(ubyte)hour, cast(ubyte)minute, cast(ubyte)second, hoffset);
	res.moffset = moffset;
	return res;
}

/++ Parse `DATE TIME` PostgreSQL textual representation into `PgSQLTimestamp`. +/
auto parsePgSQLTimestamp(ref scope const(char)[] x) {
	auto date = parseDate(x);
	x.skip();
	auto time = parsePgSQLTime(x);
	return PgSQLTimestamp(date, time);
}

private:
void skip(ref scope const(char)[] x, char ch) {
	if (!x.length || x[0] != ch)
		throw new PgSQLProtocolException("Bad datetime string format");
	x = x[1 .. $];
}

void skip(ref scope const(char)[] x) {
	if (!x.length)
		throw new PgSQLProtocolException("Bad datetime string format");
	x = x[1 .. $];
}

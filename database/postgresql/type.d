module database.postgresql.type;

import core.bitop : bsr;
import database.postgresql.protocol;
import database.postgresql.packet;
import database.postgresql.row;
import std.datetime;
import std.format : format, formattedWrite;
import std.traits;
public import database.util;

enum isValueType(T) = !is(T == struct) || is(Unqual!T == PgSQLValue) ||
	is(T : Date) || is(T : DateTime) || is(T : SysTime);

template PgTypeof(T) if (is(T == enum)) {
	static if (is(Unqual!T == PgType))
		enum PgTypeof = PgType.OID;
	else
		enum PgTypeof = PgTypeof!(OriginalType!T);
}

template PgTypeof(T) if (!is(T == enum)) {
	alias U = Unqual!T;
	static if (is(T : typeof(null)))
		enum PgTypeof = PgType.NULL;
	else static if (isIntegral!T)
		enum PgTypeof = [PgType.INT2, PgType.INT4, PgType.INT8][T.sizeof / 4];
	else static if (isSomeString!T)
		enum PgTypeof = PgType.TEXT;
	else static if (is(U == float))
		enum PgTypeof = PgType.REAL;
	else static if (is(U == double))
		enum PgTypeof = PgType.DOUBLE;
	else static if (isSomeChar!T)
		enum PgTypeof = PgType.CHAR;
	else static if (is(U == Date))
		enum PgTypeof = PgType.DATE;
	else static if (is(U == TimeOfDay) || is(U == PgSQLTime))
		enum PgTypeof = PgType.TIME;
	else static if (is(U == DateTime) || is(U == PgSQLTimestamp))
		enum PgTypeof = PgType.TIMESTAMP;
	else static if (is(U == SysTime))
		enum PgTypeof = PgType.TIMESTAMPTZ;
	else static if (is(U == ubyte[]) || is(U : ubyte[n], size_t n))
		enum PgTypeof = PgType.BYTEA;
	else
		enum PgTypeof = PgType.UNKNOWN;
}

@safe pure:

struct PgSQLValue {
	package this(PgType type, char[] str) {
		type_ = type;
		arr = cast(ubyte[])str;
	}

	this(typeof(null)) {
		type_ = PgType.NULL;
	}

	this(bool value) {
		type_ = PgType.BOOL;
		p = value ? 't' : 'f';
	}

	// dfmt off
	this(T)(T value) @trusted if (isScalarType!T && !isBoolean!T) {
		static if (isFloatingPoint!T) {
			static assert(T.sizeof <= 8, "Unsupported type: " ~ T.stringof);
			enum t = [PgType.REAL, PgType.DOUBLE][T.sizeof / 8];
		} else
			enum t = [PgType.CHAR, PgType.INT2, PgType.INT4, PgType.INT8][bsr(T.sizeof)];
		type_ = t;

		*cast(Unqual!T*)&p = value;
	}

	this(Date value) @trusted {
		type_ = PgType.DATE;
		timestamp.date = value;
	}

	this(TimeOfDay value) {
		this(PgSQLTime(value.hour, value.minute, value.second));
	}

	this(PgSQLTime value) @trusted {
		type_ = PgType.TIME;
		timestamp.time = value;
	}

	this(DateTime value) {
		this(PgSQLTimestamp(value.date, PgSQLTime(value.hour, value.minute, value.second)));
	}

	this(in SysTime value) @trusted {
		this(cast(DateTime)value);
		type_ = PgType.TIMESTAMPTZ;
	}

	this(in PgSQLTimestamp value) @trusted {
		type_ = PgType.TIMESTAMP;
		timestamp.date = value.date;
		timestamp.time = value.time;
	}

	this(const(char)[] value) @trusted {
		type_ = PgType.VARCHAR;
		arr = cast(ubyte[])value;
	}

	this(const(ubyte)[] value) @trusted {
		type_ = PgType.BYTEA;
		arr = cast(ubyte[])value;
	}

	void toString(R)(ref R app) @trusted const {
		switch(type_) with (PgType) {
		case UNKNOWN:
		case NULL:
			break;
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
		case POINT, LSEG, PATH, BOX, POLYGON, LINE:
		case TINTERVAL:
		case CIRCLE:
		case JSONB:
		case BYTEA:
			app.formattedWrite("%s", arr);
			break;
		case MONEY:
		case TEXT, NAME:
		case BIT, VARBIT:
		case NUMERIC:
		case INET, CIDR, MACADDR, MACADDR8:
		case UUID, JSON, XML:
		case CHARA, VARCHAR:
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

	string toString() const {
		import std.array : appender;

		auto app = appender!string;
		toString(app);
		return app[];
	}

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

	T get(T)(lazy T def) const => !isNull ? get!T : def;

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
		throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
	}

	T get(T : SysTime)() @trusted const {
		switch (type_) with (PgType) {
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toSysTime;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
	}

	T get(T : DateTime)() @trusted const {
		switch (type_) with (PgType) {
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toDateTime;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
	}

	T get(T : TimeOfDay)() @trusted const {
		switch (type_) with (PgType) {
		case TIME, TIMETZ:
			return time.toTimeOfDay;
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toTimeOfDay;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
	}

	T get(T : Duration)() @trusted const {
		switch (type_) with (PgType) {
		case TIME, TIMETZ:
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.time.toDuration;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
	}

	T get(T)() @trusted const if (is(T : Date)) {
		switch (type_) with (PgType) {
		case DATE:
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.date;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
	}

	T get(T)() const if (is(T == enum))
	=> cast(T)get!(OriginalType!T);

	T get(T)() const @trusted if (isArray!T && !is(T == enum)) {
		switch(type_) with (PgType) {
		case NUMERIC:
		case MONEY:
		case BIT, VARBIT:
		case INET, CIDR, MACADDR, MACADDR8:
		case UUID, JSON, XML:
		case TEXT, NAME:
		case VARCHAR, CHARA:
		case BYTEA:
			static if (isStaticArray!T)
				return cast(T)arr[0 .. T.sizeof];
			else
				return cast(T)arr.dup;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to array".format(type_.columnTypeName));
	}

	T peek(T)(lazy T def) const => !isNull ? peek!T : def;

	T peek(T)() const if (is(T == struct) || isStaticArray!T) => get!T;

	T peek(T)() @trusted const if (is(T == U[], U)) {
		switch(type_) with (PgType) {
		case NUMERIC:
		case MONEY:
		case BIT, VARBIT:
		case INET, CIDR, MACADDR, MACADDR8:
		case UUID, JSON, XML:
		case TEXT, NAME:
		case VARCHAR, CHARA:
			return cast(T)arr;
		default:
		}
		throw new PgSQLErrorException("Cannot convert %s to array".format(type_.columnTypeName));
	}

	size_t toHash() const @nogc @trusted pure nothrow
	=> *cast(size_t*)&p ^ (type_ << 24);

	@property nothrow @nogc {

		bool isNull() const => type_ == PgType.NULL;

		bool isUnknown() const => type_ == PgType.UNKNOWN;

		PgType type() const => type_;

		bool isString() const {
			switch(type_) with (PgType) {
			case NUMERIC:
			case MONEY:
			case BIT, VARBIT:
			case INET, CIDR, MACADDR, MACADDR8:
			case UUID, JSON, XML:
			case TEXT, NAME:
			case VARCHAR, CHARA:
				return true;
			default:
			}
			return false;
		}

		bool isScalar() const {
			switch(type_) with (PgType) {
			case BOOL, CHAR:
			case INT2, INT4, INT8:
			case REAL, DOUBLE:
				return true;
			default:
			}
			return false;
		}

		bool isFloat() const => type_ == PgType.REAL || type_ == PgType.DOUBLE;

		bool isTime() const => type_ == PgType.TIME || type_ == PgType.TIMETZ;

		bool isDate() const => type_ == PgType.DATE;

		bool isTimestamp() const => type_ == PgType.TIMESTAMP || type_ == PgType.TIMESTAMPTZ;
	}
	// dfmt on

private:
	static if (size_t.sizeof == 8) {
		union {
			struct {
				uint length;
				PgType type_;
				ubyte p;
			}

			ubyte[] _arr;
			PgSQLTimestamp timestamp;
		}

		@property ubyte[] arr() @trusted const {
			auto arr = cast(ubyte[])_arr;
			arr.length &= uint.max;
			return arr;
		}

		@property ubyte[] arr(ubyte[] arr) @trusted
		in (arr.length <= uint.max) {
			auto type = type_;
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

/// Field (column) description, part of RowDescription message.
struct PgSQLColumn {
	/// Name of the field
	string name;

	/// If the field can be identified as a column of a specific table,
	/// the object ID of the table; otherwise zero.
	int table;

	/// If the field can be identified as a column of a specific table,
	/// the attribute number of the column; otherwise zero.
	short columnId;

	/// The data type size (see pg_type.typlen).
	/// Note that negative values denote variable-width types.
	short length;

	/// The object ID of the field's data type.
	PgType type;

	/// The type modifier (see pg_attribute.atttypmod).
	/// The meaning of the modifier is type-specific.
	int modifier;

	/// The format code being used for the field. Currently will be zero (text)
	/// or one (binary). In a RowDescription returned from the statement variant
	/// of Describe, the format code is not yet known and will always be zero.
	FormatCode format;
}

struct PgSQLHeader {
	PgSQLColumn[] cols;
	alias cols this;

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

	this(ubyte h, ubyte m, ubyte s) {
		hour = h;
		minute = m;
		second = s;
	}

	private this(uint usec, ubyte h, ubyte m, ubyte s, byte hoffset = 0) pure {
		_usec = usec;
		hour = h;
		minute = m;
		second = s;
		this.hoffset = hoffset;
	}

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

	Duration toDuration() const => usecs((hour + hoffset) * 3600_000_000L + (
			minute + moffset) * 60_000_000L +
			second * 1_000_000L +
			usec);

	TimeOfDay toTimeOfDay() const => TimeOfDay(hour + hoffset, minute + moffset, second);

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
	Date date;
	align(size_t.sizeof) PgSQLTime time;

	SysTime toSysTime() const {
		auto datetime = DateTime(date.year, date.month, date.day, time.hour, time.minute, time
				.second);
		if (time.hoffset || time.moffset) {
			const offset = time.hoffset.hours + time.moffset.minutes;
			return SysTime(datetime, time.usec.usecs, new immutable SimpleTimeZone(offset));
		}
		return SysTime(datetime, time.usec.usecs);
	}

	TimeOfDay toTimeOfDay() const => time.toTimeOfDay();

	DateTime toDateTime() const => DateTime(date, time.toTimeOfDay());

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

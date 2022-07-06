module database.postgresql.type;

import std.algorithm;
import std.array : appender;
import std.conv : to;
import std.datetime;
import std.format: format, formattedWrite;
import std.meta : AliasSeq;
import std.traits;
import std.typecons;
import database.postgresql.protocol;
import database.postgresql.packet;
import database.postgresql.exception;
import database.postgresql.row;
public import database.util;

alias SQLName = KeyName;

enum isValueType(T) = !is(Unqual!T == struct) || is(Unqual!T == PgSQLValue) ||
	is(Unqual!T == Date) || is(Unqual!T == DateTime) || is(Unqual!T == SysTime);

struct PgSQLRawString {
	@disable this();

	this(string s) { data = s; }

	string data;
	alias data this;
}

struct PgSQLFragment {
	@disable this();

	this(string s) { data = s; }

	string data;
	alias data this;
}

struct PgSQLValue {
	package this(PgType type, in void[] str) {
		type_ = type;
		arr = cast(ubyte[])str;
	}

	this(T)(T) if (is(Unqual!T : typeof(null))) {
		type_ = PgType.NULL;
	}

	this(bool value) {
		type_ = PgType.BOOL;
		p = value ? 't' : 'f';
	}

	this(T)(T value) if (isScalarType!T && !isBoolean!T) {
		static if (is(UT == float))			type_ = PgType.REAL;
		else static if (isFloatingPoint!T) {type_ = PgType.DOUBLE;
			static assert(T.sizeof <= 8, "Unsupported type: " ~ T.stringof);
		} else static if (T.sizeof == 8)	type_ = PgType.INT8;
		else static if (T.sizeof == 4)		type_ = PgType.INT4;
		else static if (T.sizeof == 2)		type_ = PgType.INT2;
		else
			type_ = PgType.CHAR;

		*cast(Unqual!T*)&p = value;
	}

	this(T)(T value) if (is(Unqual!T == Date)) {
		type_ = PgType.DATE;
		timestamp.date = value;
	}

	this(T)(T value) if (is(Unqual!T == TimeOfDay)) {
		this(PgSQLTime(value));
	}

	this(T)(T value) if (is(Unqual!T == PgSQLTime)) {
		type_ = PgType.TIME;
		timestamp.time = value;
	}

	this(T)(T value) if (is(Unqual!T == DateTime)) {
		this(PgSQLTimestamp(value));
	}

	this(T)(T value) if (is(Unqual!T == SysTime)) {
		type_ = PgType.TIMESTAMPTZ;
		auto ts = PgSQLTimestamp(value);
		timestamp.date = ts.date;
		timestamp.time = ts.time;
	}

	this(T)(T value) if (is(Unqual!T == PgSQLTimestamp)) {
		type_ = PgType.TIMESTAMP;
		timestamp.date = value.date;
		timestamp.time = value.time;
	}

	this(T)(T value) if (isSomeString!(OriginalType!T) && typeof(T.init[0]).sizeof == 1) {
		type_ = PgType.VARCHAR;
		arr = cast(ubyte[])value;
	}

	this(T)(T value) if (is(Unqual!T == ubyte[])) {
		type_ = PgType.BYTEA;
		arr = value;
	}

	void toString(R)(ref R app) const {
		final switch(type_) with (PgType) {
		case UNKNOWN:
		case NULL:
			break;
		case BOOL:
			app.put(*cast(bool*)&p ? "TRUE" : "FALSE");
			break;
		case CHAR:
			formattedWrite(&app, "%s", *cast(ubyte*)&p);
			break;
		case INT2:
			formattedWrite(&app, "%d", *cast(short*)&p);
			break;
		case INT4:
			formattedWrite(&app, "%d", *cast(int*)&p);
			break;
		case INT8:
			formattedWrite(&app, "%d", *cast(long*)&p);
			break;
		case REAL:
			formattedWrite(&app, "%g", *cast(float*)&p);
			break;
		case DOUBLE:
			formattedWrite(&app, "%g", *cast(double*)&p);
			break;
		case POINT, LSEG, PATH, BOX, POLYGON, LINE:
		case TINTERVAL:
		case CIRCLE:
		case JSONB:
		case BYTEA:
			formattedWrite(&app, "%s", arr);
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
			goto case;
		case INTERVAL:
			break;
		}
	}

	string toString() const {
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

	T get(T)(lazy T def) const {
		return !isNull ? get!T : def;
	}

	T get(T)() const if (isScalarType!T && !is(T == enum)) {
		switch(type_) with (PgType) {
		case CHAR: return cast(T)*cast(char*)&p;
		case INT2: return cast(T)*cast(short*)&p;
		case INT4: return cast(T)*cast(int*)&p;
		case INT8: return cast(T)*cast(long*)&p;
		case REAL: return cast(T)*cast(float*)&p;
		case DOUBLE: return cast(T)*cast(double*)&p;
		default:
			throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == SysTime)) {
		switch (type_) with (PgType) {
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toSysTime;
		default:
			throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == DateTime)) {
		switch (type_) with (PgType) {
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toDateTime;
		default:
			throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == TimeOfDay)) {
		switch (type_) with (PgType) {
		case TIME, TIMETZ:
			return time.toTimeOfDay;
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.toTimeOfDay;
		default:
			throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == Duration)) {
		switch (type_) with (PgType) {
		case TIME, TIMETZ:
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.time.toDuration;
		default:
			throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == Date)) {
		switch (type_) with (PgType) {
		case DATE:
		case TIMESTAMP, TIMESTAMPTZ:
			return timestamp.date;
		default:
			throw new PgSQLErrorException("Cannot convert %s to %s".format(type_.columnTypeName, T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == enum)) {
		return cast(T)get!(OriginalType!T);
	}

	T get(T)() const if (isArray!T && !is(T == enum)) {
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
				return cast(T)arr[0..T.sizeof];
			else
				return dup(cast(T)arr);
		default:
			throw new PgSQLErrorException("Cannot convert %s to array".format(type_.columnTypeName));
		}
	}

	T peek(T)(lazy T def) const {
		return !isNull ? peek!T : def;
	}

	T peek(T)() const if (is(T == struct) || isStaticArray!T) {
		return get!T;
	}

	T peek(T)() const if (isDynamicArray!T && !is(T == enum)) {
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
			throw new PgSQLErrorException("Cannot convert %s to array".format(type_.columnTypeName));
		}
	}
	@property @safe nothrow @nogc {

		bool isNull() const { return type_ == PgType.NULL; }

		bool isUnknown() const { return type_ == PgType.UNKNOWN; }

		PgType type() const { return type_; }

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
				return false;
			}
		}

		bool isScalar() const {
			switch(type_) with (PgType) {
			case BOOL, CHAR:
			case INT2, INT4, INT8:
			case REAL, DOUBLE:
				return true;
			default:
				return false;
			}
		}

		bool isFloat() const {
			return type_ == PgType.REAL || type_ == PgType.DOUBLE;
		}

		bool isTime() const {
			return type_ == PgType.TIME || type_ == PgType.TIMETZ;
		}

		bool isDate() const {
			return type_ == PgType.DATE;
		}

		bool isTimestamp() const {
			return type_ == PgType.TIMESTAMP || type_ == PgType.TIMESTAMPTZ;
		}
	}

private:
	static if(size_t.sizeof == 8) {
		union {
			struct {
				uint length;
				PgType type_;
				ubyte p;
			}
			ubyte[] _arr;
			PgSQLTimestamp timestamp;
		}

		@property ubyte[] arr() const {
			auto arr = cast(ubyte[])_arr;
			arr.length &= uint.max;
			return arr;
		}

		@property ubyte[] arr(ubyte[] arr) in(arr.length <= uint.max) {
			auto type = type_;
			_arr = arr;
			type_ = type;
			return arr;
		}
	} else {
		PgType type_ = PgType.NULL;
		union {
			ubyte p;
			ubyte[] arr;
			PgSQLTimestamp timestamp;
		}
	}
}

struct PgSQLColumn {
	ushort length;
	FormatCode format;
	PgType type;
	int modifier;
	string name;
}

struct PgSQLHeader {
	PgSQLColumn[] cols;
	alias cols this;

	this(size_t count, ref InputPacket packet) {
		import std.array;

		cols = uninitializedArray!(PgSQLColumn[])(count);
		foreach (ref def; cols) {
			def.name = packet.eatz().idup;

			packet.skip(6);

			def.type = cast(PgType)packet.eat!uint;
			def.length = packet.eat!short;
			def.modifier = packet.eat!int;
			def.format = cast(FormatCode)packet.eat!short;
		}
	}
}

struct PgSQLTime {
	union {
		uint _usec;
		struct {
			version(LittleEndian) {
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

	@property uint usec() const { return _usec & 0xFFFFFF; }

	@property uint usec(const uint usec) in(usec <= 0xFFFFFF) {
		_usec = usec | moffset << 24;
		return usec;
	}

	invariant((_usec & 0xFFFFFF) < 1_000_000);
	invariant(hour < 24 && minute < 60 && second < 60);
	invariant(0 <= hour + hoffset && hour + hoffset < 24);
	invariant(0 <= minute + moffset && minute + moffset < 60);

	Duration toDuration() const {
		return usecs((hour + hoffset) * 3600_000_000L +
			(minute + moffset) * 60_000_000L +
			second * 1_000_000L +
			usec);
	}

	TimeOfDay toTimeOfDay() const {
		return TimeOfDay(hour + hoffset, minute + moffset, second);
	}

	void toString(W)(ref W writer) const {
		writer.formattedWrite("%02d:%02d:%02d", hour, minute, second);
		if (usec) {
			uint usecabv = usec;
			if (usecabv % 1000 == 0)
				usecabv /= 1000;
			if (usecabv % 100 == 0)
				usecabv /= 100;
			if (usecabv % 10 == 0)
				usecabv /= 10;
			writer.formattedWrite(".%d", usecabv);
		}
		if (hoffset || moffset) {
			if (hoffset < 0 || moffset < 0) {
				writer.formattedWrite("-%02d", -hoffset);
				if (moffset)
					writer.formattedWrite(":%02d", -moffset);
			} else {
				writer.formattedWrite("+%02d", hoffset);
				if (moffset)
					writer.formattedWrite(":%02d", moffset);
			}
		}
	}
}

struct PgSQLTimestamp {
	Date date;
	align(size_t.sizeof) PgSQLTime time;

	SysTime toSysTime() const {
		auto datetime = DateTime(date.year, date.month, date.day, time.hour, time.minute, time.second);
		if (time.hoffset || time.moffset) {
			const offset = time.hoffset.hours + time.moffset.minutes;
			return SysTime(datetime, time.usec.usecs, new immutable SimpleTimeZone(offset));
		}
		return SysTime(datetime, time.usec.usecs);
	}

	TimeOfDay toTimeOfDay() const { return time.toTimeOfDay(); }

	DateTime toDateTime() const {
		return DateTime(date, time.toTimeOfDay());
	}

	void toString(R)(ref R app) const {
		app.formattedWrite("%04d-%02d-%02d ", date.year, date.month, date.day);
		time.toString(app);
	}
}

private void skip(ref string x, in char ch) {
	if (!x.length || x[0] != ch)
		throw new PgSQLProtocolException("Bad datetime string format");
	x = x[1..$];
}

private void skip(ref string x) {
	if (!x.length)
		throw new PgSQLProtocolException("Bad datetime string format");
	x = x[1..$];
}

auto parseDate(ref string x) {
	int year = x.parse!int(0);
	x.skip('-');
	int month = x.parse!int(0);
	x.skip('-');
	int day = x.parse!int(0);
	return Date(year, month, day);
}

auto parsePgSQLTime(ref string x) {
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

	PgSQLTime res = {
		usecs,
		hour: cast(ubyte)hour,
		minute: cast(ubyte)minute,
		second: cast(ubyte)second,
		hoffset: hoffset
	};
	res.moffset = moffset;
	return res;
}

auto parsePgSQLTimestamp(ref string x) {
	auto date = parseDate(x);
	x.skip();
	auto time = parsePgSQLTime(x);
	return PgSQLTimestamp(date, time);
}

void eatValueText(ref InputPacket packet, in PgSQLColumn column, ref PgSQLValue value) {
	import std.array;

	auto length = packet.eat!uint;
	if (length == uint.max) {
		value = PgSQLValue(null);
		return;
	}
	auto svalue = packet.eat!string(length);
	final switch(column.type) with (PgType) {
	case UNKNOWN:
	case NULL:
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
		goto case;
	case POINT:
	case LSEG:
	case PATH:
	case BOX:
	case POLYGON:
	case LINE:
	case TINTERVAL:
	case CIRCLE:
		break;

	case NUMERIC:
	case MONEY:
	case BIT:
	case VARBIT:
	case INET:
	case CIDR:
	case MACADDR:
	case MACADDR8:
	case UUID:
	case JSON:
	case XML:
	case TEXT:
	case NAME:
	case VARCHAR:
	case CHARA:
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
	case TIME:
	case TIMETZ:
		value = PgSQLValue(parsePgSQLTime(svalue));
		break;
	case TIMESTAMP:
	case TIMESTAMPTZ:
		value = PgSQLValue(parsePgSQLTimestamp(svalue));
		goto case;
	case INTERVAL:
	case JSONB:
		break;
	}
}

private uint hexDecode(char c) @safe @nogc pure nothrow {
	return c + 9 * (c >> 6) & 15;
}
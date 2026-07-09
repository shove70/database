module database.mysql.type;

import std.algorithm;
import std.array : appender;
import std.conv : parse, to;
import std.datetime;
import std.format : format, formattedWrite;
import std.traits;
import std.typecons;
import std.variant;
import database.mysql.protocol;
import database.mysql.packet;
import database.mysql.exception;
import database.mysql.row;
public import database.util;

/++ Unwrap one level of `Nullable<T>` to the underlying type. +/
alias Unnull(N : Nullable!T, T) = T;

/++ Remove one level of `Nullable` and qualifiers. +/
alias Unnull(T) = T;

/++ Normalized helper for value-type checks on nullable inputs. +/
alias Unboth(T) = Unqual!(Unnull!T);

/++ Whether `T` is a duration-like type for MySQL mappings. +/
enum isSomeDuration(T) = is(Unboth!T == Date) || is(Unboth!T == DateTime) || is(Unboth!T == SysTime) || is(
		Unboth!T == Duration) || is(Unboth!T == TimeOfDay);

/++ Public value-type predicate used by the SQL binding layer. +/
enum isValueType(T) = isSomeDuration!(Unboth!T) || is(Unboth!T == MySQLValue) || (
		!is(Unboth!T == struct) && !is(Unboth!T == class));

/++ String wrapper for raw SQL fragments that must not be escaped. +/
struct MySQLRawString {
	@disable this();

	/++ Construct a raw SQL fragment wrapper from UTF-8 text. +/
	this(const(char)[] data) {
		data_ = data;
	}

	/++ Length of the wrapped data. +/
	@property auto length() const => data_.length;

	/++ Read-only access to the wrapped text. +/
	@property auto data() const => data_;

	private const(char)[] data_;
}

/++ Lightweight wrapper for preformatted fragments inserted verbatim. +/
struct MySQLFragment {
	@disable this();

	/++ Construct a reusable SQL fragment wrapper from UTF-8 text. +/
	this(const(char)[] data) {
		data_ = data;
	}

	/++ Length of the wrapped fragment. +/
	@property auto length() const => data_.length;

	/++ Read-only access to the wrapped fragment. +/
	@property auto data() const => data_;

	private const(char)[] data_;
}

/++ Opaque binary payload wrapper for BLOB values. +/
struct MySQLBinary {
	/++ Create a binary wrapper by viewing raw bytes of array-like input. +/
	this(T)(T[] data) {
		data_ = (cast(ubyte*)data.ptr)[0 .. typeof(T[].init[0]).sizeof * data.length];
	}

	/++ Byte length of binary payload. +/
	@property auto length() const => data_.length;

	/++ Read-only binary payload view. +/
	@property auto data() const => data_;

	private const(ubyte)[] data_;
}

/++ Runtime value container for MySQL result and bind values. +/
struct MySQLValue {
	/++ Maximum byte width required by supported value payloads. +/
	package enum BufferSize = max(ulong.sizeof, (ulong[]).sizeof, MySQLDateTime.sizeof, MySQLTime.sizeof);

	/++ Construct from raw packet bytes and a column type.

	Used for protocol decoding.
+/
	package this(const(char)[] name, ColumnTypes type, bool signed, void* ptr, size_t size) {
		assert(size <= BufferSize);
		type_ = type;
		sign_ = signed ? 0x00 : 0x80;
		if (type != ColumnTypes.MYSQL_TYPE_NULL)
			buffer_[0 .. size] = (cast(ubyte*)ptr)[0 .. size];
		name_ = name;
	}

	/++ Construct a SQL `NULL` value. +/
	this(typeof(null)) {
		type_ = ColumnTypes.MYSQL_TYPE_NULL;
		sign_ = 0x00;
	}

	/++ Copy constructor for `MySQLValue` reuse with same internal representation. +/
	this(T)(T value) if (is(Unqual!T == MySQLValue)) {
		this = value;
	}

	/++ Construct from floating-point numeric types (`float`, `double`). +/
	this(T)(T value) if (std.traits.isFloatingPoint!T) {
		alias UT = Unqual!T;

		sign_ = 0x00;
		static if (is(UT == float)) {
			type_ = ColumnTypes.MYSQL_TYPE_FLOAT;
			buffer_[0 .. T.sizeof] = (cast(ubyte*)&value)[0 .. T.sizeof];
		} else static if (is(UT == double)) {
			type_ = ColumnTypes.MYSQL_TYPE_DOUBLE;
			buffer_[0 .. T.sizeof] = (cast(ubyte*)&value)[0 .. T.sizeof];
		} else {
			type_ = ColumnTypes.MYSQL_TYPE_DOUBLE;
			auto data = cast(double)value;
			buffer_[0 .. typeof(data).sizeof] = (cast(ubyte*)&data)[0 .. typeof(data).sizeof];
		}
	}

	/++ Construct from integral values (`byte`/`short`/`int`/`long` and unsigned variants). +/
	this(T)(T value) if (__traits(isIntegral, T)) {
		static if (T.sizeof == 8) {
			type_ = ColumnTypes.MYSQL_TYPE_LONGLONG;
		} else static if (T.sizeof == 4) {
			type_ = ColumnTypes.MYSQL_TYPE_LONG;
		} else static if (T.sizeof == 2) {
			type_ = ColumnTypes.MYSQL_TYPE_SHORT;
		} else {
			type_ = ColumnTypes.MYSQL_TYPE_TINY;
		}

		sign_ = isUnsigned!T ? 0x80 : 0x00;
		buffer_[0 .. T.sizeof] = (cast(ubyte*)&value)[0 .. T.sizeof];
	}

	/++ Construct from date/time values (`Date`, `DateTime`, `SysTime`). +/
	this(T)(T value) if (is(T : Date) || is(T : DateTime) || is(T : SysTime)) {
		type_ = ColumnTypes.MYSQL_TYPE_TIMESTAMP;
		sign_ = 0x00;
		(*cast(MySQLDateTime*)buffer_) = MySQLDateTime.from(value);
	}

	/++ Construct from duration-like values (`Duration`, `TimeOfDay`). +/
	this(T)(T value) if (is(T : Duration) || is(T : TimeOfDay)) {
		type_ = ColumnTypes.MYSQL_TYPE_TIME;
		sign_ = 0x00;
		(*cast(MySQLTime*)buffer_) = MySQLTime.from(value);
	}

	/++ Construct from textual data for textual SQL types. +/
	this(T)(T value) if (isSomeString!(OriginalType!T)) {
		static assert(typeof(T.init[0]).sizeof == 1, "Unsupported string type: " ~ T.stringof);

		type_ = ColumnTypes.MYSQL_TYPE_STRING;
		sign_ = 0x80;

		auto slice = value[0 .. $];
		buffer_.ptr[0 .. typeof(slice).sizeof] = (cast(ubyte*)&slice)[0 .. typeof(slice).sizeof];
	}

	/++ Construct from binary payload wrappers and store as `BLOB`. +/
	this(T)(T value) if (is(Unqual!T == MySQLBinary)) {
		type_ = ColumnTypes.MYSQL_TYPE_BLOB;
		sign_ = 0x80;
		buffer_.ptr[0 .. (ubyte[]).sizeof] = (cast(ubyte*)&value.data_)[0 .. (ubyte[]).sizeof];
	}

	/++ Serialize this value into SQL representation using protocol-compatible formatting. +/
	void toString(Appender)(ref Appender app) const {
		final switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			break;
		case MYSQL_TYPE_TINY:
			if (isSigned)
				formattedWrite(&app, "%d", *cast(ubyte*)buffer_.ptr);
			else
				formattedWrite(&app, "%d", *cast(byte*)buffer_.ptr);
			break;
		case MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT:
			if (isSigned)
				formattedWrite(&app, "%d", *cast(short*)buffer_.ptr);
			else
				formattedWrite(&app, "%d", *cast(ushort*)buffer_.ptr);
			break;
		case MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG:
			if (isSigned)
				formattedWrite(&app, "%d", *cast(int*)buffer_.ptr);
			else
				formattedWrite(&app, "%d", *cast(uint*)buffer_.ptr);
			break;
		case MYSQL_TYPE_LONGLONG:
			if (isSigned)
				formattedWrite(&app, "%d", *cast(long*)buffer_.ptr);
			else
				formattedWrite(&app, "%d", *cast(ulong*)buffer_.ptr);
			break;
		case MYSQL_TYPE_FLOAT:
			float f = *cast(float*)buffer_.ptr;
			long l = cast(long)f;
			f -= l;
			string str = to!string(l) ~ ((f > 0) ? f.to!string[1 .. $] : "");
			app.put(str);
			//formattedWrite(&app, "%g", *cast(float*)buffer_.ptr);
			break;
		case MYSQL_TYPE_DOUBLE:
			double d = *cast(double*)buffer_.ptr;
			long l = cast(long)d;
			d -= l;
			string str = to!string(l) ~ ((d > 0) ? d.to!string[1 .. $] : "");
			app.put(str);
			//formattedWrite(&app, "%g", *cast(double*)buffer_.ptr);
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
			MYSQL_TYPE_BLOB:
			app.put(*cast(string*)buffer_.ptr);
			break;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_GEOMETRY:
			formattedWrite(&app, "%s", *cast(ubyte[]*)buffer_.ptr);
			break;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			formattedWrite(&app, "%s", (*cast(MySQLTime*)buffer_.ptr).to!Duration());
			break;
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			DateTime dt = (*cast(MySQLDateTime*)buffer_.ptr).to!DateTime();
			app.put(dt.date().toISOExtString() ~ ' ' ~ dt.timeOfDay().toISOExtString());
			//formattedWrite(&app, "%s", (*cast(MySQLDateTime*)buffer_.ptr).to!DateTime());
			break;
		}
	}

	/++ Convert to string using allocator-backed rendering. +/
	string toString() const {
		auto app = appender!string;
		toString(app);
		return app.data;
	}

	/++ Compare two `MySQLValue` values with type-aware coercion. +/
	bool opEquals(MySQLValue other) const {
		if (isString && other.isString) {
			return peek!string == other.peek!string;
		} else if (isScalar && other.isScalar) {
			if (isFloatingPoint || other.isFloatingPoint)
				return get!double == other.get!double;
			if (isSigned || other.isSigned)
				return get!long == other.get!long;
			return get!ulong == other.get!ulong;
		} else if (isTime && other.isTime) {
			return get!Duration == other.get!Duration;
		} else if (isTimestamp && other.isTimestamp) {
			return get!SysTime == other.get!SysTime;
		} else if (isNull && other.isNull) {
			return true;
		}

		return false;
	}

	/++ Get value with a fallback for `NULL`.

	The overload set also includes scalar/date/time/duration/array/enum conversions for
	`T` and nullable values.
+/
	T get(T)(lazy T def) const => !isNull ? get!T : def;

	/++ Convert non-enum scalar numeric values from the stored representation. +/
	T get(T)() const if (isScalarType!T && !is(T == enum)) {
		switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_TINY:
			return cast(T)(*cast(ubyte*)buffer_.ptr);
		case MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT:
			return cast(T)(*cast(ushort*)buffer_.ptr);
		case MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG:
			return cast(T)(*cast(uint*)buffer_.ptr);
		case MYSQL_TYPE_LONGLONG:
			return cast(T)(*cast(ulong*)buffer_.ptr);
		case MYSQL_TYPE_FLOAT:
			return cast(T)(*cast(float*)buffer_.ptr);
		case MYSQL_TYPE_DOUBLE:
			return cast(T)(*cast(double*)buffer_.ptr);
		default:
			throw new MySQLErrorException("Cannot convert '%s' from %s to %s".format(name_,
					columnTypeName(type_), T.stringof));
		}
	}

	/++ Convert to date/time values (`Date`, `DateTime`, `SysTime`). +/
	T get(T)() const
	if (is(T : SysTime) || is(T : DateTime) || is(T : Date)) {
		switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return (*cast(MySQLDateTime*)buffer_.ptr).to!T;
		default:
			throw new MySQLErrorException("Cannot convert '%s' from %s to %s".format(name_,
					columnTypeName(type_), T.stringof));
		}
	}

	/++ Convert to `TimeOfDay`. +/
	T get(T)() const if (is(T : TimeOfDay)) {
		switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return (*cast(MySQLDateTime*)buffer_.ptr).to!T;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return (*cast(MySQLTime*)buffer_.ptr).to!T;
		default:
			throw new MySQLErrorException("Cannot convert '%s' from %s to %s".format(name_,
					columnTypeName(type_), T.stringof));
		}
	}

	/++ Convert to `Duration` for duration-like SQL types. +/
	T get(T)() const if (is(T : Duration)) {
		switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return (*cast(MySQLTime*)buffer_.ptr).to!T;
		default:
			throw new MySQLErrorException("Cannot convert '%s' from %s to %s".format(name_,
					columnTypeName(type_), T.stringof));
		}
	}

	/++ Convert enum values through the underlying stored scalar type. +/
	T get(T)() const if (is(T == enum)) => cast(T)get!(OriginalType!T);

	/++ Convert string/binary and generic static/dynamic arrays. +/
	T get(T)() const if (isArray!T && !is(T == enum)) {
		switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_SET,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_STRING,
			MYSQL_TYPE_JSON,
			MYSQL_TYPE_NEWDECIMAL,
			MYSQL_TYPE_DECIMAL:
			return (*cast(T*)buffer_.ptr).dup;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB,
			MYSQL_TYPE_BLOB,
			MYSQL_TYPE_GEOMETRY:
			return (*cast(T*)buffer_.ptr).dup;
		default:
			throw new MySQLErrorException("Cannot convert '%s' from %s to %s".format(name_,
					columnTypeName(type_), T.stringof));
		}
	}

	/++ Convert nullable containers; returns `T.init` for SQL `NULL`. +/
	T get(T)() const if (isInstanceOf!(Nullable, T)) {
		if (type_ == ColumnTypes.MYSQL_TYPE_NULL)
			return T.init;
		return T(get!(typeof(T.init.get)));
	}

	/++ Access values with fallback and typed `peek` overloads.

	`peek` overloads share the same type conversion behavior as `get`, but keep
	the same fallback semantics as `MySQLValue.this`.
+/
	T peek(T)(lazy T def) const => !isNull ? peek!T : def;

	/++ Peek scalar values by type. +/
	T peek(T)() const if (isScalarType!T) => get!T;

	/++ Peek date/time values by type. +/
	T peek(T)() const if (is(T : SysTime) || is(T : DateTime) ||
		is(T : Date) || is(T : TimeOfDay)) => get!T;

	/++ Peek duration values by type. +/
	T peek(T)() const if (is(T : Duration)) => get!T;

	/++ Peek array values by type. +/
	T peek(T)() const if (isArray!T) {
		switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_SET,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_STRING,
			MYSQL_TYPE_JSON,
			MYSQL_TYPE_NEWDECIMAL,
			MYSQL_TYPE_DECIMAL:
			return (*cast(T*)buffer_.ptr);
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB,
			MYSQL_TYPE_BLOB,
			MYSQL_TYPE_GEOMETRY:
			return (*cast(T*)buffer_.ptr);
		default:
			throw new MySQLErrorException("Cannot convert '%s' from %s to %s".format(name_,
					columnTypeName(type_), T.stringof));
		}
	}

	/++ Whether this is SQL `NULL`. +/
	bool isNull() const => type_ == ColumnTypes.MYSQL_TYPE_NULL;

	/++ Access underlying MySQL column type enum. +/
	ColumnTypes type() const => type_;

	/++ Whether the value has a signed numeric representation. +/
	bool isSigned() const => sign_ == 0x00;

	/++ Whether this value should be treated as string-like data. +/
	bool isString() const {
		final switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY,
			MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT,
			MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG,
			MYSQL_TYPE_LONGLONG,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_DOUBLE:
			return false;
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
			MYSQL_TYPE_BLOB:
			return true;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	/++ Whether this value is numeric scalar. +/
	bool isScalar() const {
		final switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY,
			MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT,
			MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG,
			MYSQL_TYPE_LONGLONG,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_DOUBLE:
			return true;
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
			MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	/++ Whether this value is floating-point (`FLOAT`/`DOUBLE`). +/
	bool isFloatingPoint() const {
		final switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY,
			MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT,
			MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG,
			MYSQL_TYPE_LONGLONG:
			return false;
		case MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_DOUBLE:
			return true;
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
			MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	/++ Whether this value is `TIME`/`TIME2`. +/
	bool isTime() const {
		final switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY,
			MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT,
			MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG,
			MYSQL_TYPE_LONGLONG,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_DOUBLE:
			return false;
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
			MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return true;
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	/++ Alias for duration-like values (`isTime`). +/
	alias isDuration = isTime;

	/++ Whether this value is a date/time family type. +/
	bool isDateTime() const {
		final switch (type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY,
			MYSQL_TYPE_YEAR,
			MYSQL_TYPE_SHORT,
			MYSQL_TYPE_INT24,
			MYSQL_TYPE_LONG,
			MYSQL_TYPE_LONGLONG,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_DOUBLE:
			return false;
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
			MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT,
			MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME,
			MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE,
			MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_DATETIME,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP,
			MYSQL_TYPE_TIMESTAMP2:
			return true;
		}
	}

	alias isTimestamp = isDateTime;

private:

	ColumnTypes type_ = ColumnTypes.MYSQL_TYPE_NULL;
	ubyte sign_;
	ubyte[6] pad_;
	ubyte[BufferSize] buffer_;
	const(char)[] name_;
}

struct MySQLColumn {
	/++ Column length in bytes as defined by server metadata. +/
	uint length;
	/++ Column flags from column definition. +/
	ushort flags;
	/++ Number of decimal digits, when applicable. +/
	ubyte decimals;
	/++ Column type from protocol metadata. +/
	ColumnTypes type;
	/++ Column name from result set metadata. +/
	string name;
}

/++ Convenience alias for a list of column descriptors. +/
alias MySQLHeader = MySQLColumn[];

/++ MySQL duration-like type used by TIME columns. +/
struct MySQLTime {
	/++ Elapsed day component. +/
	uint days;
	/++ Signed flag (`1` when negative). +/
	ubyte negative;
	/++ Hour component in the stored range. +/
	ubyte hours;
	/++ Minute component. +/
	ubyte mins;
	/++ Second component. +/
	ubyte secs;
	/++ Sub-second microseconds. +/
	uint usecs;

	/++ Convert to typed temporal duration target. +/
	auto to(T : Duration)() const {
		auto total = days * 86400_000_000L +
			hours * 3600_000_000L +
			mins * 60_000_000L +
			secs * 1_000_000L +
			usecs;
		return cast(T)dur!"usecs"(negative ? -total : total);
	}

	/++ Convert to `TimeOfDay` preserving local clock fields. +/
	auto to(T : TimeOfDay)() const
		=> cast(T)TimeOfDay(hours, mins, secs);

	/++ Convert from `Duration` by decomposing days/hours/min/sec/usecs. +/
	static MySQLTime from(Duration duration) {
		MySQLTime time;
		duration.abs.split!("days", "hours", "minutes", "seconds", "usecs")(time.days, time.hours, time.mins, time
				.secs, time.usecs);
		time.negative = duration.isNegative ? 1 : 0;
		return time;
	}

	/++ Convert from `TimeOfDay` (zero days, non-negative). +/
	static MySQLTime from(TimeOfDay tod) {
		MySQLTime time;
		time.hours = tod.hour;
		time.mins = tod.minute;
		time.secs = tod.second;
		return time;
	}
}

/++ Encode `MySQLTime` into protocol wire format. +/
void putMySQLTime(ref OutputPacket packet, in MySQLTime time) {
	if (time.days || time.hours || time.mins || time.mins || time.usecs) {
		auto usecs = time.usecs != 0;
		packet.put!ubyte(usecs ? 12 : 8);
		packet.put!ubyte(time.negative);
		packet.put!uint(time.days);
		packet.put!ubyte(time.hours);
		packet.put!ubyte(time.mins);
		packet.put!ubyte(time.secs);
		if (usecs)
			packet.put!uint(time.usecs);
	} else {
		packet.put!ubyte(0);
	}
}

/++ Decode protocol-encoded `MySQLTime`. +/
auto eatMySQLTime(ref InputPacket packet) {
	MySQLTime time;
	switch (packet.eat!ubyte) {
	case 12:
		time.negative = packet.eat!ubyte;
		time.days = packet.eat!uint;
		time.hours = packet.eat!ubyte;
		time.mins = packet.eat!ubyte;
		time.secs = packet.eat!ubyte;
		time.usecs = packet.eat!uint;
		break;
	case 8:
		time.negative = packet.eat!ubyte;
		time.days = packet.eat!uint;
		time.hours = packet.eat!ubyte;
		time.mins = packet.eat!ubyte;
		time.secs = packet.eat!ubyte;
		break;
	case 0:
		break;
	default:
		throw new MySQLProtocolException("Bad time struct format");
	}

	return time;
}

/++ Packed SQL datetime value used for temporal MySQL fields. +/
struct MySQLDateTime {
	/++ Year component. +/
	ushort year;
	/++ Month component. +/
	ubyte month;
	/++ Day component. +/
	ubyte day;
	/++ Hour component. +/
	ubyte hour;
	/++ Minute component. +/
	ubyte min;
	/++ Second component. +/
	ubyte sec;
	/++ Microseconds fraction. +/
	uint usec;

	/++ Return false when the value is unset/invalid. +/
	bool valid() const => month != 0;

	/++ Convert to `SysTime` in UTC-based interpretation. +/
	T to(T : SysTime)() const {
		assert(valid());
		return cast(T)SysTime(DateTime(year, month, day, hour, min, sec), usec.dur!"usecs", UTC());
	}

	/++ Convert to `DateTime`. +/
	T to(T : DateTime)() const {
		assert(valid());
		return cast(T)DateTime(year, month, day, hour, min, sec);
	}

	/++ Convert to `Date`. +/
	T to(T : Date)() const {
		assert(valid());
		return cast(T)Date(year, month, day);
	}

	/++ Convert to `TimeOfDay`. +/
	T to(T : TimeOfDay)() const => cast(T)TimeOfDay(hour, min, sec);

	/++ Build `MySQLDateTime` from `SysTime`. +/
	static MySQLDateTime from(SysTime sysTime) {
		MySQLDateTime time;

		auto dateTime = cast(DateTime)sysTime;
		time.year = dateTime.year;
		time.month = dateTime.month;
		time.day = dateTime.day;
		time.hour = dateTime.hour;
		time.min = dateTime.minute;
		time.sec = dateTime.second;
		time.usec = cast(int)sysTime.fracSecs.total!"usecs";

		return time;
	}

	/++ Build `MySQLDateTime` from `DateTime`. +/
	static MySQLDateTime from(DateTime dateTime) {
		MySQLDateTime time;

		time.year = dateTime.year;
		time.month = dateTime.month;
		time.day = dateTime.day;
		time.hour = dateTime.hour;
		time.min = dateTime.minute;
		time.sec = dateTime.second;

		return time;
	}

	/++ Build `MySQLDateTime` from `Date` (time set to midnight). +/
	static MySQLDateTime from(Date date) {
		MySQLDateTime time;

		time.year = date.year;
		time.month = date.month;
		time.day = date.day;

		return time;
	}
}

/++ Serialize `MySQLDateTime` and write its length marker. +/
void putMySQLDateTime(ref OutputPacket packet, in MySQLDateTime time) {
	auto marker = packet.marker!ubyte;
	ubyte length;

	if (time.year || time.month || time.day) {
		length = 4;
		packet.put!ushort(time.year);
		packet.put!ubyte(time.month);
		packet.put!ubyte(time.day);

		if (time.hour || time.min || time.sec || time.usec) {
			length = 7;
			packet.put!ubyte(time.hour);
			packet.put!ubyte(time.min);
			packet.put!ubyte(time.sec);

			if (time.usec) {
				length = 11;
				packet.put!uint(time.usec);
			}
		}
	}

	packet.put!ubyte(marker, length);
}

/++ Parse protocol payload bytes into `MySQLDateTime` with supported wire lengths. +/
auto eatMySQLDateTime(ref InputPacket packet) {
	MySQLDateTime time;
	switch (packet.eat!ubyte) {
	case 11:
		time.year = packet.eat!ushort;
		time.month = packet.eat!ubyte;
		time.day = packet.eat!ubyte;
		time.hour = packet.eat!ubyte;
		time.min = packet.eat!ubyte;
		time.sec = packet.eat!ubyte;
		time.usec = packet.eat!uint;
		break;
	case 7:
		time.year = packet.eat!ushort;
		time.month = packet.eat!ubyte;
		time.day = packet.eat!ubyte;
		time.hour = packet.eat!ubyte;
		time.min = packet.eat!ubyte;
		time.sec = packet.eat!ubyte;
		break;
	case 4:
		time.year = packet.eat!ushort;
		time.month = packet.eat!ubyte;
		time.day = packet.eat!ubyte;
		break;
	case 0:
		break;
	default:
		throw new MySQLProtocolException("Bad datetime struct format");
	}

	return time;
}

/++ Consume a mandatory separator character in textual datetime parsing. +/
private void skip(ref const(char)[] x, char ch) {
	if (x.length && x.ptr[0] == ch) {
		x = x[1 .. $];
	} else {
		throw new MySQLProtocolException("Bad datetime string format");
	}
}

/++ Parse string `HH:MM:SS(.ffffff)` into `MySQLTime`. +/
auto parseMySQLTime(const(char)[] x) {
	MySQLTime time;

	auto hours = x.parse!int;
	if (hours < 0) {
		time.negative = 1;
		hours = -hours;
	}
	time.days = hours / 24;
	time.hours = cast(ubyte)(hours % 24);
	x.skip(':');
	time.mins = x.parse!ubyte;
	x.skip(':');
	time.secs = x.parse!ubyte;
	if (x.length) {
		x.skip('.');
		time.usecs = x.parse!uint;
		switch (6 - max(6, x.length)) {
			case 0: break;
			case 1: time.usecs *= 10; break;
			case 2: time.usecs *= 100; break;
			case 3: time.usecs *= 1_000; break;
			case 4: time.usecs *= 10_000; break;
			case 5: time.usecs *= 100_000; break;
			default: assert(0, "Bad datetime string format");
		}
	}

	return time;
}

/++ Parse SQL datetime text into `MySQLDateTime`. +/
auto parseMySQLDateTime(const(char)[] x) {
	MySQLDateTime time;

	time.year = x.parse!ushort;
	x.skip('-');
	time.month = x.parse!ubyte;
	x.skip('-');
	time.day = x.parse!ubyte;
	if (x.length) {
		x.skip(' ');
		time.hour = x.parse!ubyte;
		x.skip(':');
		time.min = x.parse!ubyte;
		x.skip(':');
		time.sec = x.parse!ubyte;

		if (x.length) {
			x.skip('.');
			time.usec = x.parse!uint;
			switch (6 - max(6, x.length)) {
				case 0: break;
				case 1: time.usec *= 10; break;
				case 2: time.usec *= 100; break;
				case 3: time.usec *= 1_000; break;
				case 4: time.usec *= 10_000; break;
				case 5: time.usec *= 100_000; break;
				default: assert(0, "Bad datetime string format");
			}
		}
	}

	return time;
}

/++ Decode binary protocol value from packet into `MySQLValue` based on column metadata. +/
MySQLValue eatValue(ref InputPacket packet, ref const MySQLColumn column) {
	auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
	final switch (column.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		return MySQLValue(column.name, column.type, signed, null, 0);
	case MYSQL_TYPE_TINY:
		auto x = packet.eat!ubyte;
		return MySQLValue(column.name, column.type, signed, &x, 1);
	case MYSQL_TYPE_YEAR,
		MYSQL_TYPE_SHORT:
		auto x = packet.eat!ushort;
		return MySQLValue(column.name, column.type, signed, &x, 2);
	case MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG:
		auto x = packet.eat!uint;
		return MySQLValue(column.name, column.type, signed, &x, 4);
	case MYSQL_TYPE_DOUBLE,
		MYSQL_TYPE_LONGLONG:
		auto x = packet.eat!ulong;
		return MySQLValue(column.name, column.type, signed, &x, 8);
	case MYSQL_TYPE_FLOAT:
		auto x = packet.eat!float;
		return MySQLValue(column.name, column.type, signed, &x, 4);
	case MYSQL_TYPE_SET,
		MYSQL_TYPE_ENUM,
		MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING,
		MYSQL_TYPE_STRING,
		MYSQL_TYPE_JSON,
		MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_DECIMAL:
		auto x = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
		return MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
	case MYSQL_TYPE_BIT,
		MYSQL_TYPE_TINY_BLOB,
		MYSQL_TYPE_MEDIUM_BLOB,
		MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_BLOB,
		MYSQL_TYPE_GEOMETRY:
		auto x = packet.eat!(const(ubyte)[])(cast(size_t)packet.eatLenEnc());
		return MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
	case MYSQL_TYPE_TIME,
		MYSQL_TYPE_TIME2:
		auto x = eatMySQLTime(packet);
		return MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
	case MYSQL_TYPE_DATE,
		MYSQL_TYPE_NEWDATE,
		MYSQL_TYPE_DATETIME,
		MYSQL_TYPE_DATETIME2,
		MYSQL_TYPE_TIMESTAMP,
		MYSQL_TYPE_TIMESTAMP2:
		auto x = eatMySQLDateTime(packet);
		return x.valid() ? MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof) : MySQLValue(column.name, ColumnTypes
				.MYSQL_TYPE_NULL, signed, null, 0);
	}
}

/++ Decode textual protocol value from packet into `MySQLValue` based on column metadata. +/
MySQLValue eatValueText(ref InputPacket packet, ref const MySQLColumn column) {
	auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
	auto svalue = (column.type != ColumnTypes.MYSQL_TYPE_NULL) ? cast(string)(
		packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc())) : string.init;
	final switch (column.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		return MySQLValue(column.name, column.type, signed, null, 0);
	case MYSQL_TYPE_TINY:
		auto x = svalue.ptr[0] == '-' ? cast(ubyte)(-1 * svalue[1 .. $].to!byte) : svalue.to!ubyte;
		return MySQLValue(column.name, column.type, signed, &x, 1);
	case MYSQL_TYPE_YEAR,
		MYSQL_TYPE_SHORT:
		auto x = svalue.ptr[0] == '-' ? cast(ushort)(-1 * svalue[1 .. $].to!short)
			: svalue.to!ushort;
		return MySQLValue(column.name, column.type, signed, &x, 2);
	case MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG:
		auto x = svalue.ptr[0] == '-' ? cast(uint)(-svalue[1 .. $].to!int) : svalue.to!uint;
		return MySQLValue(column.name, column.type, signed, &x, 4);
	case MYSQL_TYPE_LONGLONG:
		auto x = svalue.ptr[0] == '-' ? cast(ulong)(-svalue[1 .. $].to!long) : svalue.to!ulong;
		return MySQLValue(column.name, column.type, signed, &x, 8);
	case MYSQL_TYPE_DOUBLE:
		auto x = svalue.to!double;
		return MySQLValue(column.name, column.type, signed, &x, 8);
	case MYSQL_TYPE_FLOAT:
		auto x = svalue.to!float;
		return MySQLValue(column.name, column.type, signed, &x, 4);
	case MYSQL_TYPE_SET,
		MYSQL_TYPE_ENUM,
		MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING,
		MYSQL_TYPE_STRING,
		MYSQL_TYPE_JSON,
		MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_DECIMAL:
		return MySQLValue(column.name, column.type, signed, &svalue, typeof(svalue).sizeof);
	case MYSQL_TYPE_BIT,
		MYSQL_TYPE_TINY_BLOB,
		MYSQL_TYPE_MEDIUM_BLOB,
		MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_BLOB,
		MYSQL_TYPE_GEOMETRY:
		return MySQLValue(column.name, column.type, signed, &svalue, typeof(svalue).sizeof);
	case MYSQL_TYPE_TIME,
		MYSQL_TYPE_TIME2:
		auto x = parseMySQLTime(svalue);
		return MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
	case MYSQL_TYPE_DATE,
		MYSQL_TYPE_NEWDATE,
		MYSQL_TYPE_DATETIME,
		MYSQL_TYPE_DATETIME2,
		MYSQL_TYPE_TIMESTAMP,
		MYSQL_TYPE_TIMESTAMP2:
		auto x = parseMySQLDateTime(svalue);
		return x.valid() ? MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof) : MySQLValue(column.name, ColumnTypes
				.MYSQL_TYPE_NULL, signed, null, 0);
	}
}

/++ Generic `Variant` dispatcher for emitting protocol type bytes only. +/
void putValueType(T)(ref OutputPacket packet, T value) if (is(Unqual!T == Variant)) {
	if (!value.hasValue) {
		putValueType(packet, MySQLValue(null));
	} else if (value.type == typeid(string)) {
		putValueType(packet, value.get!string);
	} else if (value.type == typeid(dstring)) {
		putValueType(packet, value.get!dstring);
	} else if (value.type == typeid(wstring)) {
		putValueType(packet, value.get!wstring);
	} else if (value.type == typeid(short)) {
		putValueType(packet, value.get!short);
	} else if (value.type == typeid(int)) {
		putValueType(packet, value.get!int);
	} else if (value.type == typeid(long)) {
		putValueType(packet, value.get!long);
	} else if (value.type == typeid(ushort)) {
		putValueType(packet, value.get!ushort);
	} else if (value.type == typeid(uint)) {
		putValueType(packet, value.get!uint);
	} else if (value.type == typeid(ulong)) {
		putValueType(packet, value.get!ulong);
	} else if (value.type == typeid(float)) {
		putValueType(packet, value.get!float);
	} else if (value.type == typeid(double)) {
		putValueType(packet, value.get!double);
	} else if (value.type == typeid(byte)) {
		putValueType(packet, value.get!byte);
	} else if (value.type == typeid(ubyte)) {
		putValueType(packet, value.get!ubyte);
	} else if (value.type == typeid(bool)) {
		putValueType(packet, value.get!bool);
	} else if (value.type == typeid(Date)) {
		putValueType(packet, value.get!Date);
	} else if (value.type == typeid(DateTime)) {
		putValueType(packet, value.get!DateTime);
	} else if (value.type == typeid(SysTime)) {
		putValueType(packet, value.get!SysTime);
	} else if (value.type == typeid(Duration)) {
		putValueType(packet, value.get!Duration);
	} else if (value.type == typeid(MySQLBinary)) {
		putValueType(packet, value.get!MySQLBinary);
	} else if (value.type == typeid(MySQLValue)) {
		putValueType(packet, value.get!MySQLValue);
	} else {
		throw new Exception("exists unkown type at Variant[]: " ~ value.type.toString());
	}
}

/++ Generic `Variant` encoding dispatch for supported runtime value types. +/
void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == Variant)) {
	if (!value.hasValue) {
		putValue(packet, MySQLValue(null));
	} else if (value.type == typeid(string)) {
		putValue(packet, value.get!string);
	} else if (value.type == typeid(dstring)) {
		putValue(packet, value.get!dstring);
	} else if (value.type == typeid(wstring)) {
		putValue(packet, value.get!wstring);
	} else if (value.type == typeid(short)) {
		putValue(packet, value.get!short);
	} else if (value.type == typeid(int)) {
		putValue(packet, value.get!int);
	} else if (value.type == typeid(long)) {
		putValue(packet, value.get!long);
	} else if (value.type == typeid(ushort)) {
		putValue(packet, value.get!ushort);
	} else if (value.type == typeid(uint)) {
		putValue(packet, value.get!uint);
	} else if (value.type == typeid(ulong)) {
		putValue(packet, value.get!ulong);
	} else if (value.type == typeid(float)) {
		putValue(packet, value.get!float);
	} else if (value.type == typeid(double)) {
		putValue(packet, value.get!double);
	} else if (value.type == typeid(byte)) {
		putValue(packet, value.get!byte);
	} else if (value.type == typeid(ubyte)) {
		putValue(packet, value.get!ubyte);
	} else if (value.type == typeid(bool)) {
		putValue(packet, value.get!bool);
	} else if (value.type == typeid(Date)) {
		putValue(packet, value.get!Date);
	} else if (value.type == typeid(DateTime)) {
		putValue(packet, value.get!DateTime);
	} else if (value.type == typeid(SysTime)) {
		putValue(packet, value.get!SysTime);
	} else if (value.type == typeid(Duration)) {
		putValue(packet, value.get!Duration);
	} else if (value.type == typeid(MySQLBinary)) {
		putValue(packet, value.get!MySQLBinary);
	} else if (value.type == typeid(MySQLValue)) {
		putValue(packet, value.get!MySQLValue);
	} else {
		throw new Exception("exists unkown type at Variant[]: " ~ value.type.toString());
	}
}

/++ Emit protocol type metadata for datetime-like values. +/
void putValueType(T)(ref OutputPacket packet, T value)
if (is(T : Date) || is(T : DateTime) || is(T : SysTime)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TIMESTAMP);
	packet.put!ubyte(0x80);
}

/++ Encode `Date`/`DateTime`/`SysTime` datetime values to packet format. +/
void putValue(T)(ref OutputPacket packet, T value)
if (is(T : Date) || is(T : DateTime) || is(T : SysTime)) {
	putMySQLDateTime(packet, MySQLDateTime.from(value));
}

/++ Emit type marker for duration-like values and write `TIME` metadata. +/
void putValueType(T)(ref OutputPacket packet, T value) if (is(T : Duration)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TIME);
	packet.put!ubyte(0x00);
}

/++ Encode `Duration`/`TimeOfDay` values. +/
void putValue(T)(ref OutputPacket packet, T value) if (is(T : Duration)) {
	putMySQLTime(packet, MySQLTime.from(value));
}

/++ Emit integer type metadata for signed/unsigned integral values. +/
void putValueType(T)(ref OutputPacket packet, T value) if (__traits(isIntegral, T)) {
	enum ubyte sign = isUnsigned!T ? 0x80 : 0x00;

	static if (T.sizeof == 8) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_LONGLONG);
		packet.put!ubyte(sign);
	} else static if (T.sizeof == 4) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_LONG);
		packet.put!ubyte(sign);
	} else static if (T.sizeof == 2) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_SHORT);
		packet.put!ubyte(sign);
	} else {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TINY);
		packet.put!ubyte(sign);
	}
}

/++ Encode integral values using the smallest available MySQL integer type. +/
void putValue(T)(ref OutputPacket packet, T value) if (__traits(isIntegral, T)) {
	static if (T.sizeof == 8) {
		packet.put!ulong(value);
	} else static if (T.sizeof == 4) {
		packet.put!uint(value);
	} else static if (T.sizeof == 2) {
		packet.put!ushort(value);
	} else {
		packet.put!ubyte(value);
	}
}

/++ Emit floating-point metadata for `float`/`double` values. +/
void putValueType(T)(ref OutputPacket packet, T value) if (isFloatingPoint!T) {
	alias UT = Unqual!T;

	enum ubyte sign = 0x00;

	static if (is(UT == float)) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_FLOAT);
		packet.put!ubyte(sign);
	} else {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_DOUBLE);
		packet.put!ubyte(sign);
	}
}

/++ Encode `float`/`double` values in protocol form. +/
void putValue(T)(ref OutputPacket packet, T value) if (isFloatingPoint!T) {
	alias UT = Unqual!T;

	static if (is(UT == float)) {
		packet.put!float(value);
	} else {
		packet.put!double(cast(double)value);
	}
}

/++ Emit string/binary type marker for textual input types. +/
void putValueType(T)(ref OutputPacket packet, T value)
if (isSomeString!(OriginalType!T)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_STRING);
	packet.put!ubyte(0x80);
}

/++ Encode strings with length-encoded payload. +/
void putValue(T)(ref OutputPacket packet, T value)
if (isSomeString!(OriginalType!T)) {
	ulong size = value.length * T.init[0].sizeof;
	packet.putLenEnc(size);
	packet.put(value);
}

/++ Encode arrays element-by-element for type metadata. +/
void putValueType(T)(ref OutputPacket packet, T value)
if (isArray!T && !isSomeString!(OriginalType!T)) {
	foreach (ref item; value)
		putValueType(packet, item);
}

/++ Encode array elements recursively. +/
void putValue(T)(ref OutputPacket packet, T value)
if (isArray!T && !isSomeString!(OriginalType!T)) {
	foreach (ref item; value)
		putValue(packet, item);
}

/++ Emit BLOB metadata for `MySQLBinary` values. +/
void putValueType(T)(ref OutputPacket packet, T value)
if (is(Unqual!T == MySQLBinary)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_BLOB);
	packet.put!ubyte(0x80);
}

/++ Write `MySQLBinary` payload with length encoding. +/
void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == MySQLBinary)) {
	ulong size = value.length;
	packet.putLenEnc(size);
	packet.put(value.data);
}

/++ Emit raw `MySQLValue` type/signature before payload serialization. +/
void putValueType(T)(ref OutputPacket packet, T value)
if (is(Unqual!T == MySQLValue)) {
	packet.put!ubyte(value.type_);
	packet.put!ubyte(value.sign_);
}

/++ Serialize the concrete `MySQLValue` payload according to its `ColumnTypes`. +/
void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == MySQLValue)) {
	final switch (value.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		break;
	case MYSQL_TYPE_TINY:
		packet.put!ubyte(*cast(ubyte*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_YEAR,
		MYSQL_TYPE_SHORT:
		packet.put!ushort(*cast(ushort*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG:
		packet.put!uint(*cast(uint*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_LONGLONG:
		packet.put!ulong(*cast(ulong*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_DOUBLE:
		packet.put!double(*cast(double*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_FLOAT:
		packet.put!float(*cast(float*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_SET,
		MYSQL_TYPE_ENUM,
		MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING,
		MYSQL_TYPE_STRING,
		MYSQL_TYPE_JSON,
		MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_DECIMAL,
		MYSQL_TYPE_BIT,
		MYSQL_TYPE_TINY_BLOB,
		MYSQL_TYPE_MEDIUM_BLOB,
		MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_BLOB,
		MYSQL_TYPE_GEOMETRY:
		packet.putLenEnc((*cast(ubyte[]*)value.buffer_.ptr).length);
		packet.put(*cast(ubyte[]*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_TIME,
		MYSQL_TYPE_TIME2:
		packet.putMySQLTime(*cast(MySQLTime*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_DATE,
		MYSQL_TYPE_NEWDATE,
		MYSQL_TYPE_DATETIME,
		MYSQL_TYPE_DATETIME2,
		MYSQL_TYPE_TIMESTAMP,
		MYSQL_TYPE_TIMESTAMP2:
		packet.putMySQLDateTime(*cast(MySQLDateTime*)value.buffer_.ptr);
		break;
	}
}

/++ Emit `NULL` value type metadata. +/
void putValueType(ref OutputPacket packet, typeof(null)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_NULL);
	packet.put!ubyte(0x00);
}

/++ Skip writing data for null `VALUE` because metadata already emitted. +/
void putValue(ref OutputPacket packet, typeof(null)) {
}

/++ Write nullable wrappers by emitting null marker or wrapped value metadata. +/
void putValueType(T)(ref OutputPacket packet, T value)
if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T)) {
	if (value.isNull) {
		putValueType(packet, null);
	} else {
		putValueType(packet, value.get);
	}
}

/++ Write nullable wrappers by emitting null payload or wrapped value. +/
void putValue(T)(ref OutputPacket packet, T value)
if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T)) {
	if (value.isNull) {
		putValue(packet, null);
	} else {
		putValue(packet, value.get);
	}
}

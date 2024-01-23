module database.mysql.appender;

import std.datetime;
import std.traits;
import std.typecons;
import database.mysql.type;

void appendValues(R, T)(ref R appender, T values)
if (isArray!T && !isSomeString!(OriginalType!T)) {
	foreach (i, value; values) {
		appendValue(appender, value);
		if (i != values.length - 1)
			appender.put(',');
	}
}

void appendValue(R)(ref R appender, typeof(null)) {
	appender.put("null");
}

void appendValue(R, T)(ref R appender, T value)
if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T)) {
	appendValue(appender, value.isNull ? null : value.get);
}

void appendValue(R, T)(ref R appender, T value) if (isScalarType!T) {
	import std.conv : to;

	appender.put(cast(ubyte[])to!string(value));
}

void appendValue(R)(ref R appender, SysTime value) {
	value = value.toUTC;

	auto hour = value.hour;
	auto minute = value.minute;
	auto second = value.second;
	auto usec = value.fracSecs.total!"usecs";

	formattedWrite(appender, "%04d%02d%02d", value.year, value.month, value.day);
	if (hour | minute | second | usec) {
		formattedWrite(appender, "%02d%02d%02d", hour, minute, second);
		if (usec)
			formattedWrite(appender, ".%06d", usec);
	}
}

void appendValue(R)(ref R appender, DateTime value) {
	auto hour = value.hour;
	auto minute = value.minute;
	auto second = value.second;

	if (hour | minute | second) {
		formattedWrite(appender, "%04d%02d%02d%02d%02d%02d", value.year, value.month, value.day, hour, minute, second);
	} else {
		formattedWrite(appender, "%04d%02d%02d", value.year, value.month, value.day);
	}
}

void appendValue(R)(ref R appender, TimeOfDay value) {
	formattedWrite(appender, "%02d%02d%02d", value.hour, value.minute, value.second);
}

void appendValue(R)(ref R appender, Date value) {
	formattedWrite(appender, "%04d%02d%02d", value.year, value.month, value.day);
}

void appendValue(R)(ref R appender, Duration value) {
	auto parts = value.split();
	if (parts.days) {
		appender.put('\'');
		formattedWrite(appender, "%d ", parts.days);
	}
	formattedWrite(appender, "%02d%02d%02d", parts.hours, parts.minutes, parts.seconds);
	if (parts.usecs)
		formattedWrite(appender, ".%06d ", parts.usecs);
	if (parts.days)
		appender.put('\'');
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == MySQLFragment)) {
	appender.put(cast(char[])value.data);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == MySQLRawString)) {
	appender.put('\'');
	appender.put(cast(char[])value.data);
	appender.put('\'');
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == MySQLBinary)) {
	appendValue(appender, value.data);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == MySQLValue)) {
	final switch (value.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		appender.put("null");
		break;
	case MYSQL_TYPE_TINY:
		if (value.isSigned) {
			appendValue(appender, value.peek!byte);
		} else {
			appendValue(appender, value.peek!ubyte);
		}
		break;
	case MYSQL_TYPE_YEAR,
		MYSQL_TYPE_SHORT:
		if (value.isSigned) {
			appendValue(appender, value.peek!short);
		} else {
			appendValue(appender, value.peek!ushort);
		}
		break;
	case MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG:
		if (value.isSigned) {
			appendValue(appender, value.peek!int);
		} else {
			appendValue(appender, value.peek!uint);
		}
		break;
	case MYSQL_TYPE_LONGLONG:
		if (value.isSigned) {
			appendValue(appender, value.peek!long);
		} else {
			appendValue(appender, value.peek!ulong);
		}
		break;
	case MYSQL_TYPE_DOUBLE:
		appendValue(appender, value.peek!double);
		break;
	case MYSQL_TYPE_FLOAT:
		appendValue(appender, value.peek!float);
		break;
	case MYSQL_TYPE_SET,
		MYSQL_TYPE_ENUM,
		MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING,
		MYSQL_TYPE_STRING,
		MYSQL_TYPE_JSON,
		MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_DECIMAL:
		appendValue(appender, value.peek!(char[]));
		break;
	case MYSQL_TYPE_BIT,
		MYSQL_TYPE_TINY_BLOB,
		MYSQL_TYPE_MEDIUM_BLOB,
		MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_BLOB,
		MYSQL_TYPE_GEOMETRY:
		appendValue(appender, value.peek!(ubyte[]));
		break;
	case MYSQL_TYPE_TIME,
		MYSQL_TYPE_TIME2:
		appendValue(appender, value.peek!Duration);
		break;
	case MYSQL_TYPE_DATE,
		MYSQL_TYPE_NEWDATE,
		MYSQL_TYPE_DATETIME,
		MYSQL_TYPE_DATETIME2,
		MYSQL_TYPE_TIMESTAMP,
		MYSQL_TYPE_TIMESTAMP2:
		appendValue(appender, value.peek!SysTime);
		break;
	}
}

void appendValue(R, T)(ref R appender, T value)
if (isArray!T && (is(Unqual!(typeof(T.init[0])) == ubyte) || is(Unqual!(typeof(T.init[0])) == char))) {
	appender.put('\'');
	auto ptr = value.ptr;
	auto end = value.ptr + value.length;
	while (ptr != end) {
		switch (*ptr) {
		case '\\',
			'\'':
			appender.put('\\');
			goto default;
		default:
			appender.put(*ptr++);
		}
	}
	appender.put('\'');
}

module database.postgresql.appender;

import std.conv;
import std.datetime;
import std.format;
import std.traits;
import database.postgresql.protocol;
import database.postgresql.type;
public import database.util : appendValue, appendValues;

void appendValue(R, T)(ref R appender, T value) if (isScalarType!T) {
	static if (isBoolean!T)
		appender.put(value ? "'t'" : "'f'");
	else
		appender.put(cast(ubyte[])value.to!string);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == SysTime)) {
	value = value.toUTC;

	auto hour = value.hour;
	auto minute = value.minute;
	auto second = value.second;
	auto usec = value.fracSecs.total!"usecs";

	appender.formattedWrite("'%04d%02d%02d", value.year, value.month, value.day);
	if (hour || minute || second || usec) {
		appender.formattedWrite(" %02d%02d%02d", hour, minute, second);
		if (usec)
			appender.formattedWrite(".%06d", usec);
	}
	appender.put('\'');
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == DateTime)) {
	auto hour = value.hour;
	auto minute = value.minute;
	auto second = value.second;

	if (hour || minute || second)
		appender.formattedWrite("'%04d%02d%02d%02d%02d%02d'", value.year, value.month, value.day, hour, minute, second);
	else
		appender.formattedWrite("'%04d%02d%02d'", value.year, value.month, value.day);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == TimeOfDay)) {
	appender.formattedWrite("'%02d%02d%02d'", value.hour, value.minute, value.second);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == Date)) {
	appender.formattedWrite("'%04d%02d%02d'", value.year, value.month, value.day);
}

void appendValue(R, T)(ref R appender, T value) if (isSomeString!T) {
	appender.put('\'');
	auto ptr = value.ptr;
	auto end = value.ptr + value.length;
	while (ptr != end) {
		switch(*ptr) {
		case '\\':
		case '\'':
			appender.put('\\');
			goto default;
		default:
			appender.put(*ptr++);
		}
	}
	appender.put('\'');
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == ubyte[])) {
	appender.formattedWrite("'\\x%(%02x%)'", value);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == PgSQLRawString)) {
	appender.put('\'');
	appender.put(cast(char[])value.data);
	appender.put('\'');
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == PgSQLFragment)) {
	appender.put(cast(char[])value.data);
}

void appendValue(R, T)(ref R appender, T value) if (is(Unqual!T == PgSQLValue)) {
	final switch(value.type) with (PgType) {
		case UNKNOWN:
		case NULL:
			appender.put("null");
			break;
		case CHAR:
			appendValue(appender, value.peek!char);
			break;
		case BOOL:
			appendValue(appender, value.peek!bool);
			break;
		case INT2:
			appendValue(appender, value.peek!short);
			break;
		case INT4:
			appendValue(appender, value.peek!int);
			break;
		case INT8:
			appendValue(appender, value.peek!long);
			break;
		case REAL:
			appendValue(appender, value.peek!float);
			break;
		case DOUBLE:
			appendValue(appender, value.peek!double);
			break;
		case POINT:
		case LSEG:
		case PATH:
		case BOX:
		case POLYGON:
		case LINE:
		case TINTERVAL:
		case INTERVAL:
		case CIRCLE:
		case BYTEA:
		case JSONB:
			appendValue(appender, value.peek!(ubyte[]));
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
			appendValue(appender, value.peek!(char[]));
			break;
		case DATE:
			appendValue(appender, value.peek!Date);
			break;
		case TIMETZ:
		case TIME:
			appendValue(appender, value.peek!TimeOfDay);
			break;
		case TIMESTAMP:
			appendValue(appender, value.peek!DateTime);
			break;
		case TIMESTAMPTZ:
			appendValue(appender, value.peek!SysTime);
			break;
	}
}

module postgresql.appender;

import std.conv;
import std.datetime;
import std.format;
import std.traits;
import std.typecons;

import postgresql.protocol;
import postgresql.type;

void appendValues(Appender, T)(ref Appender appender, T values) if (isArray!T && !isSomeString!(OriginalType!T))
{
    foreach (size_t i, value; values)
    {
        appendValue(appender, value);
        if (i != values.length-1)
            appender.put(',');
    }
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == typeof(null)))
{
    appender.put("null");
}

void appendValue(Appender, T)(ref Appender appender, T value) if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T))
{
    if (value.isNull)
    {
        appendValue(appender, null);
    }
    else
    {
        appendValue(appender, value.get);
    }
}

void appendValue(Appender, T)(ref Appender appender, T value) if (isScalarType!T)
{
    static if (isBoolean!T)
    {
        appender.put(value ? "'t'" : "'f'");
    }
    else
    {
        appender.put(cast(ubyte[])to!string(value));
    }
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == SysTime))
{
    value = value.toUTC;

    auto hour = value.hour;
    auto minute = value.minute;
    auto second = value.second;
    auto usec = value.fracSecs.total!"usecs";

    formattedWrite(appender, "'%04d%02d%02d", value.year, value.month, value.day);
    if (hour | minute | second | usec)
    {
        formattedWrite(appender, " %02d%02d%02d", hour, minute, second);
        if (usec)
            formattedWrite(appender, ".%06d", usec);
    }
    appender.put('\'');
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == DateTime))
{
    auto hour = value.hour;
    auto minute = value.minute;
    auto second = value.second;

    if (hour | minute | second)
    {
        formattedWrite(appender, "'%04d%02d%02d%02d%02d%02d'", value.year, value.month, value.day, hour, minute, second);
    }
    else
    {
        formattedWrite(appender, "'%04d%02d%02d'", value.year, value.month, value.day);
    }
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == TimeOfDay))
{
    formattedWrite(appender, "'%02d%02d%02d'", value.hour, value.minute, value.second);
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == Date))
{
    formattedWrite(appender, "'%04d%02d%02d'", value.year, value.month, value.day);
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == PgSQLFragment))
{
    appender.put(cast(char[])value.data);
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == PgSQLRawString))
{
    appender.put('\'');
    appender.put(cast(char[])value.data);
    appender.put('\'');
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == PgSQLBinary))
{
    appendValue(appender, value.data);
}

void appendValue(Appender, T)(ref Appender appender, T value) if (is(Unqual!T == PgSQLValue))
{
    final switch(value.type) with (PgColumnTypes)
    {
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

void appendValue(Appender, T)(ref Appender appender, T value) if (isArray!T && (is(Unqual!(typeof(T.init[0])) == ubyte) || is(Unqual!(typeof(T.init[0])) == char)))
{
    appender.put('\'');
    auto ptr = value.ptr;
    auto end = value.ptr + value.length;
    while (ptr != end)
    {
        switch(*ptr)
        {
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

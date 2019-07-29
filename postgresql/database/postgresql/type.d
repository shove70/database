module database.postgresql.type;

import std.algorithm;
import std.array : appender;
import std.conv : parse, to;
import std.datetime;
import std.datetime.timezone;
import std.format: format, formattedWrite;
import std.traits;
import std.typecons;

import database.postgresql.protocol;
import database.postgresql.packet;
import database.postgresql.exception;
import database.postgresql.row;

struct IgnoreAttribute {}
struct OptionalAttribute {}
struct NameAttribute { const(char)[] name; }
struct UnCamelCaseAttribute {}
struct TableNameAttribute {const(char)[] name;}

@property TableNameAttribute tableName(const(char)[] name)
{
    return TableNameAttribute(name);
}

@property IgnoreAttribute ignore()
{
    return IgnoreAttribute();
}

@property OptionalAttribute optional()
{
    return OptionalAttribute();
}

@property NameAttribute as(const(char)[] name)
{
    return NameAttribute(name);
}

@property UnCamelCaseAttribute uncamel()
{
    return UnCamelCaseAttribute();
}

template isValueType(T)
{
    static if (is(Unqual!T == struct) && !is(Unqual!T == PgSQLValue) && !is(Unqual!T == Date) && !is(Unqual!T == DateTime) && !is(Unqual!T == SysTime))
    {
        enum isValueType = false;
    }
    else
    {
        enum isValueType = true;
    }
}

template isWritableDataMember(T, string Member)
{
    static if (is(TypeTuple!(__traits(getMember, T, Member))))
    {
        enum isWritableDataMember = false;
    }
    else static if (!is(typeof(__traits(getMember, T, Member))))
    {
        enum isWritableDataMember = false;
    }
    else static if (is(typeof(__traits(getMember, T, Member)) == void))
    {
        enum isWritableDataMember = false;
    }
    else static if (is(typeof(__traits(getMember, T, Member)) == enum))
    {
        enum isWritableDataMember = true;
    }
    else static if (hasUDA!(__traits(getMember, T, Member), IgnoreAttribute))
    {
        enum isWritableDataMember = false;
    }
    else static if (isArray!(typeof(__traits(getMember, T, Member))) && !is(typeof(typeof(__traits(getMember, T, Member)).init[0]) == ubyte) && !is(typeof(__traits(getMember, T, Member)) == string))
    {
        enum isWritableDataMember = false;
    }
    else static if (isAssociativeArray!(typeof(__traits(getMember, T, Member))))
    {
        enum isWritableDataMember = false;
    }
    else static if (isSomeFunction!(typeof(__traits(getMember, T, Member))))
    {
        enum isWritableDataMember = false;
    }
    else static if (!is(typeof((){ T x = void; __traits(getMember, x, Member) = __traits(getMember, x, Member); }())))
    {
        enum isWritableDataMember = false;
    }
    else static if ((__traits(getProtection, __traits(getMember, T, Member)) != "public") && (__traits(getProtection, __traits(getMember, T, Member)) != "export"))
    {
        enum isWritableDataMember = false;
    }
    else
    {
        enum isWritableDataMember = true;
    }
}

template isReadableDataMember(T, string Member)
{
    static if (is(TypeTuple!(__traits(getMember, T, Member))))
    {
        enum isReadableDataMember = false;
    }
    else static if (!is(typeof(__traits(getMember, T, Member))))
    {
        enum isReadableDataMember = false;
    }
    else static if (is(typeof(__traits(getMember, T, Member)) == void))
    {
        enum isReadableDataMember = false;
    }
    else static if (is(typeof(__traits(getMember, T, Member)) == enum))
    {
        enum isReadableDataMember = true;
    }
    else static if (hasUDA!(__traits(getMember, T, Member), IgnoreAttribute))
    {
        enum isReadableDataMember = false;
    }
    else static if (isArray!(typeof(__traits(getMember, T, Member))) && !is(typeof(typeof(__traits(getMember, T, Member)).init[0]) == ubyte) && !is(typeof(__traits(getMember, T, Member)) == string))
    {
        enum isReadableDataMember = false;
    }
    else static if (isAssociativeArray!(typeof(__traits(getMember, T, Member))))
    {
        enum isReadableDataMember = false;
    }
    else static if (isSomeFunction!(typeof(__traits(getMember, T, Member)))  /* && return type is valueType*/ )
    {
        enum isReadableDataMember = true;
    }
    else static if (!is(typeof((){ T x = void; __traits(getMember, x, Member) = __traits(getMember, x, Member); }())))
    {
        enum isReadableDataMember = false;
    }
    else static if ((__traits(getProtection, __traits(getMember, T, Member)) != "public") && (__traits(getProtection, __traits(getMember, T, Member)) != "export"))
    {
        enum isReadableDataMember = false;
    }
    else
    {
        enum isReadableDataMember = true;
    }
}

struct PgSQLRawString
{
    @disable this();

    this(const(char)[] data)
    {
        data_ = data;
    }

    @property auto length() const
    {
        return data_.length;
    }

    @property auto data() const
    {
        return data_;
    }

    private const(char)[] data_;
}

struct PgSQLFragment
{
    @disable this();

    this(const(char)[] data)
    {
        data_ = data;
    }

    @property auto length() const
    {
        return data_.length;
    }

    @property auto data() const
    {
        return data_;
    }

    private const(char)[] data_;
}

struct PgSQLBinary
{
    this(T)(T[] data)
    {
        data_ = (cast(ubyte*)data.ptr)[0..typeof(T[].init[0]).sizeof * data.length];
    }

    @property auto length() const
    {
        return data_.length;
    }

    @property auto data() const
    {
        return data_;
    }

    private const(ubyte)[] data_;
}

struct PgSQLValue
{
    package enum BufferSize = max(ulong.sizeof, (ulong[]).sizeof, SysTime.sizeof, DateTime.sizeof, Date.sizeof);
    package this(const(char)[] name, PgColumnTypes type, void* ptr, size_t size)
    {
        assert(size <= BufferSize);
        type_ = type;
        if (type != PgColumnTypes.NULL)
            buffer_[0..size] = (cast(ubyte*)ptr)[0..size];
        name_ = name;
    }

    this(T)(T) if (is(Unqual!T == typeof(null)))
    {
        type_ = PgColumnTypes.NULL;
    }

    this(T)(T value) if (is(Unqual!T == PgSQLValue))
    {
        this = value;
    }

    this(T)(T value) if (std.traits.isFloatingPoint!T)
    {
        alias UT = Unqual!T;

        static if (is(UT == float))
        {
            type_ = PgColumnTypes.REAL;
            buffer_[0..T.sizeof] = (cast(ubyte*)&value)[0..T.sizeof];
        }
        else static if (is(UT == double))
        {
            type_ = PgColumnTypes.DOUBLE;
            buffer_[0..T.sizeof] = (cast(ubyte*)&value)[0..T.sizeof];
        }
        else
        {
            type_ = PgColumnTypes.DOUBLE;
            auto data = cast(double)value;
            buffer_[0..typeof(data).sizeof] = (cast(ubyte*)&data)[0..typeof(data).sizeof];
        }
    }

    this(T)(T value) if (isIntegral!T || isBoolean!T)
    {
        alias UT = Unqual!T;

        static if (is(UT == long) || is(UT == ulong))
        {
            type_ = PgColumnTypes.INT8;
        }
        else static if (is(UT == int) || is(UT == uint) || is(UT == dchar))
        {
            type_ = PgColumnTypes.INT4;
        }
        else static if (is(UT == short) || is(UT == ushort) || is(UT == wchar))
        {
            type_ = PgColumnTypes.INT2;
        }
        else static if (is(UT == char) || is(UT == byte) || is(UT == ubyte))
        {
            type_ = PgColumnTypes.CHAR;
        }
        else
        {
            type_ = PgColumnTypes.BOOLEAN;
        }

        buffer_[0..T.sizeof] = (cast(ubyte*)&value)[0..T.sizeof];
    }

    this(T)(T value) if (is(Unqual!T == Date))
    {
        type_ = ColumnTypes.DATE;
        (*cast(PgSQLDate*)buffer_) = PgSQLDate(value);
    }

    this(T)(T value) if (is(Unqual!T == TimeOfDay))
    {
        type_ = ColumnTypes.TIME;
        (*cast(PgSQLTime*)buffer_) = PgSQLTime(value);
    }

    this(T)(T value) if (is(Unqual!T == DateTime))
    {
        type_ = ColumnTypes.TIMESTAMP;
        (*cast(PgSQLTimestamp*)buffer_) = PgSQLTimestamp(value);
    }

    this(T)(T value) if (is(Unqual!T == SysTime))
    {
        type_ = ColumnTypes.TIMESTAMPTZ;
        (*cast(PgSQLTimestamp*)buffer_) = PgSQLTimestamp(value);
    }

    this(T)(T value) if (isSomeString!(OriginalType!T))
    {
        static assert(typeof(T.init[0]).sizeof == 1, fomat("Unsupported string type: %s", T.stringof));

        type_ = PgColumnTypes.VARCHAR;

        auto slice = value[0..$];
        buffer_.ptr[0..typeof(slice).sizeof] = (cast(ubyte*)&slice)[0..typeof(slice).sizeof];
    }

    this(T)(T value) if (is(Unqual!T == PgSQLBinary))
    {
        type_ = PgColumnTypes.BYTEA;
        buffer_.ptr[0..(ubyte[]).sizeof] = (cast(ubyte*)&value.data_)[0..(ubyte[]).sizeof];
    }

    void toString(Appender)(ref Appender app) const
    {
        final switch(type_) with (PgColumnTypes)
        {
        case UNKNOWN:
        case NULL:
            break;
        case BOOL:
            app.put(*cast(bool*)buffer_.ptr ? "TRUE" : "FALSE");
            break;
        case CHAR:
            formattedWrite(&app, "%s", *cast(ubyte*)buffer_.ptr);
            break;
        case INT2:
            formattedWrite(&app, "%d", *cast(short*)buffer_.ptr);
            break;
        case INT4:
            formattedWrite(&app, "%d", *cast(int*)buffer_.ptr);
            break;
        case INT8:
            formattedWrite(&app, "%d", *cast(long*)buffer_.ptr);
            break;
        case REAL:
            formattedWrite(&app, "%g", *cast(float*)buffer_.ptr);
            break;
        case DOUBLE:
            formattedWrite(&app, "%g", *cast(double*)buffer_.ptr);
            break;
        case POINT:
        case LSEG:
        case PATH:
        case BOX:
        case POLYGON:
        case LINE:
        case TINTERVAL:
        case CIRCLE:
        case JSONB:
        case BYTEA:
            formattedWrite(&app, "%s", *cast(ubyte[]*)buffer_.ptr);
            break;

        case MONEY:
        case TEXT:
        case NAME:
        case BIT:
        case VARBIT:
        case NUMERIC:
        case UUID:
        case MACADDR:
        case MACADDR8:
        case INET:
        case CIDR:
        case JSON:
        case XML:
        case CHARA:
        case VARCHAR:
            formattedWrite(&app, "%s", *cast(const(char)[]*)buffer_.ptr);
            break;
        case DATE:
            (*cast(PgSQLDate*)buffer_.ptr).toString(app);
            break;
        case TIMETZ:
        case TIME:
            (*cast(PgSQLTime*)buffer_.ptr).toString(app);
            break;
        case TIMESTAMP:
        case TIMESTAMPTZ:
            (*cast(PgSQLTimestamp*)buffer_.ptr).toString(app);
            break;
        case INTERVAL:
            break;
        }
    }

    string toString() const
    {
        auto app = appender!string;
        toString(app);
        return app.data;
    }

    bool opEquals(PgSQLValue other) const
    {
        if (isString && other.isString)
        {
            return peek!string == other.peek!string;
        }
        else if (isScalar == other.isScalar)
        {
            if (isFloatingPoint || other.isFloatingPoint)
                return get!double == other.get!double;
            return get!long == other.get!long;
        }
        else if (isTime == other.isTime)
        {
//             return get!Duration == other.get!Duration;
//        }
//        else if (isTimestamp == other.isTimestamp)
//        {
//             return get!SysTime == other.get!SysTime;
        }
        else if (isNull == other.isNull)
        {
            return true;
        }
        return false;
    }

    T get(T)(lazy T def) const
    {
        return !isNull ? get!T : def;
    }

    T get(T)() const if (isScalarType!T && !is(T == enum))
    {
        switch(type_) with (PgColumnTypes)
        {
        case CHAR:
            return cast(T)(*cast(char*)buffer_.ptr);
        case INT2:
            return cast(T)(*cast(short*)buffer_.ptr);
        case INT4:
            return cast(T)(*cast(int*)buffer_.ptr);
        case INT8:
            return cast(T)(*cast(long*)buffer_.ptr);
        case REAL:
            return cast(T)(*cast(float*)buffer_.ptr);
        case DOUBLE:
            return cast(T)(*cast(double*)buffer_.ptr);
        default:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
        }
    }

    T get(T)() const if (is(Unqual!T == SysTime))
    {
        switch (type_) with (PgColumnTypes)
        {
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return (*cast(PgSQLTimestamp*)buffer_.ptr).toSysTime;
        default:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
        }
    }

    T get(T)() const if (is(Unqual!T == DateTime))
    {
        switch (type_) with (PgColumnTypes) {
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return (*cast(PgSQLTimestamp*)buffer_.ptr).toDateTime;
        default:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
        }
    }

    T get(T)() const if (is(Unqual!T == TimeOfDay))
    {
        switch (type_) with (PgColumnTypes) {
        case TIME:
        case TIMETZ:
            return (*cast(PgSQLTime*)buffer_.ptr).toTimeOfDay;
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return (*cast(PgSQLTimestamp*)buffer_.ptr).toTimeOfDay;
        default:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
        }
    }

    T get(T)() const if (is(Unqual!T == Date))
    {
        switch (type_) with (PgColumnTypes)
        {
        case DATE:
            return (*cast(PgSQLDate*)buffer_.ptr).toDate;
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return (*cast(PgSQLTimestamp*)buffer_.ptr).toDate;
        default:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
        }
    }

    T get(T)() const if (is(Unqual!T == enum))
    {
        return cast(T)get!(OriginalType!T);
    }

    T get(T)() const if (isArray!T && !is(T == enum))
    {
        final switch(type_) with (PgColumnTypes)
        {
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
            return (*cast(T*)buffer_.ptr).dup;
        case UNKNOWN:
        case NULL:
        case BOOL:
        case CHAR:
        case INT2:
        case INT4:
        case INT8:
        case REAL:
        case DOUBLE:
        case POINT:
        case LSEG:
        case PATH:
        case BOX:
        case POLYGON:
        case LINE:
        case TINTERVAL:
        case CIRCLE:
        case BYTEA:
        case DATE:
        case TIME:
        case TIMETZ:
        case TIMESTAMP:
        case TIMESTAMPTZ:
        case INTERVAL:
        case JSONB:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to array", name_, columnTypeName(type_)));
        }
    }

    T peek(T)(lazy T def) const
    {
        return !isNull ? peek!(T) : def;
    }

    T peek(T)() const if (isScalarType!T)
    {
        return get!(T);
    }

    T peek(T)() const if (is(Unqual!T == SysTime) || is(Unqual!T == DateTime) || is(Unqual!T == Date) || is(Unqual!T == TimeOfDay))
    {
        return get!(T);
    }

    T peek(T)() const if (isArray!T && !is(T == enum))
    {
        final switch(type_) with (PgColumnTypes) {
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
            return *cast(T*)buffer_.ptr;
        case UNKNOWN:
        case NULL:
        case BOOL:
        case CHAR:
        case INT2:
        case INT4:
        case INT8:
        case REAL:
        case DOUBLE:
        case POINT:
        case LSEG:
        case PATH:
        case BOX:
        case POLYGON:
        case LINE:
        case TINTERVAL:
        case CIRCLE:
        case BYTEA:
        case DATE:
        case TIME:
        case TIMETZ:
        case TIMESTAMP:
        case TIMESTAMPTZ:
        case INTERVAL:
        case JSONB:
            throw new PgSQLErrorException(format("Cannot convert '%s' from %s to array", name_, columnTypeName(type_)));
        }
    }

    bool isNull() const
    {
        return type_ == PgColumnTypes.NULL;
    }

    bool isUnknown() const
    {
        return type_ == PgColumnTypes.UNKNOWN;
    }

    PgColumnTypes type() const
    {
        return type_;
    }

    bool isString() const
    {
        final switch(type_) with (PgColumnTypes)
        {
        case UNKNOWN:
        case NULL:
        case BOOL:
        case CHAR:
        case INT2:
        case INT4:
        case INT8:
        case REAL:
        case DOUBLE:
        case POINT:
        case LSEG:
        case PATH:
        case BOX:
        case POLYGON:
        case LINE:
        case TINTERVAL:
        case CIRCLE:
        case BYTEA:
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
            return true;
        case DATE:
        case TIME:
        case TIMETZ:
        case TIMESTAMP:
        case TIMESTAMPTZ:
        case INTERVAL:
        case JSONB:
            return false;
        }
        return false;
    }

    bool isScalar() const
    {
        final switch(type_) with (PgColumnTypes)
        {
        case UNKNOWN:
            return false;
        case NULL:
            return false;
        case BOOL:
        case CHAR:
        case INT2:
        case INT4:
        case INT8:
        case REAL:
        case DOUBLE:
            return true;
        case POINT:
        case LSEG:
        case PATH:
        case BOX:
        case POLYGON:
        case LINE:
        case MONEY:
        case TINTERVAL:
        case CIRCLE:
        case MACADDR:
        case INET:
        case CIDR:
        case JSON:
        case XML:
        case TEXT:
        case NAME:
        case MACADDR8:
        case BYTEA:
        case CHARA:
        case VARCHAR:
        case DATE:
        case TIME:
        case TIMETZ:
        case TIMESTAMP:
        case TIMESTAMPTZ:
        case INTERVAL:
        case BIT:
        case VARBIT:
        case NUMERIC:
        case UUID:
        case JSONB:
            return false;
        }
    }

    bool isFloatingPoint() const
    {
        return (type_ == PgColumnTypes.REAL) || (type_ == PgColumnTypes.DOUBLE);
    }

    bool isTime() const
    {
        return (type_ == PgColumnTypes.TIME) || (type_ == PgColumnTypes.TIMETZ);
    }

    bool isDate() const
    {
        return (type_ == PgColumnTypes.DATE);
    }

    bool isTimestamp() const
    {
        return (type_ == PgColumnTypes.TIMESTAMP) || (type_ == PgColumnTypes.TIMESTAMPTZ);
    }

private:

    PgColumnTypes type_ = PgColumnTypes.NULL;
    ubyte[7] pad_;
    ubyte[BufferSize] buffer_;
    const(char)[] name_;
}

struct PgSQLColumn
{
    ushort length;
    FormatCode format;
    PgColumnTypes type;
    int modifier;
    const(char)[] name;
}

alias PgSQLHeader = PgSQLColumn[];

struct PgSQLDate
{
    ushort year;
    ubyte month;
    ubyte day;

    Date toDate() const
    {
        return Date(year, month, day);
    }

    void toString(W)(ref W writer) const
    {
        formattedWrite(writer, "%04d-%02d-%02d", year, month, day);
    }
}

struct PgSQLTime
{
    uint usec;
    ubyte hour;
    ubyte minute;
    ubyte second;
    byte hoffset;
    byte moffset;

    Duration toDuration() const
    {
        auto total = (cast(int)hour + cast(int)hoffset) * 3600_000_000L +
            (cast(int)minute + cast(int)moffset) * 60_000_000L +
            second * 1_000_000L +
            usec;
        return dur!"usecs"(total);
    }

    TimeOfDay toTimeOfDay() const
    {
        return TimeOfDay(cast(int)hour + cast(int)hoffset, cast(int)minute + cast(int)moffset, second);
    }

    void toString(W)(ref W writer) const
    {
        formattedWrite(writer, "%02d:%02d:%02d", hour, minute, second);
        if (usec)
        {
            uint usecabv = usec;
            if ((usecabv % 1000) == 0)
                usecabv /= 1000;
            if ((usecabv % 100) == 0)
                usecabv /= 100;
            if ((usecabv % 10) == 0)
                usecabv /= 10;
            formattedWrite(writer, ".%d", usecabv);
        }

        if (hoffset | moffset)
        {
            if ((hoffset < 0) || (moffset < 0))
            {
                formattedWrite(writer, "-%02d", -cast(int)(this.hoffset));
                if (moffset)
                    formattedWrite(writer, ":%02d", -cast(int)(this.moffset));
            }
            else
            {
                formattedWrite(writer, "+%02d", hoffset);
                if (moffset)
                    formattedWrite(writer, ":%02d", moffset);
            }
        }
    }
}

struct PgSQLTimestamp
{
    PgSQLDate date;
    PgSQLTime time;

    SysTime toSysTime() const
    {
        if (time.hoffset | time.moffset)
        {
            const offset = time.hoffset.hours + time.moffset.minutes;
            return SysTime(DateTime(date.year, date.month, date.day, time.hour, time.minute, time.second), time.usec.usecs, new immutable(SimpleTimeZone)(offset));
        }
        else
        {
            return SysTime(DateTime(date.year, date.month, date.day, time.hour, time.minute, time.second), time.usec.usecs);
        }
    }

    TimeOfDay toTimeOfDay() const
    {
        return time.toTimeOfDay();
    }

    Date toDate() const
    {
        return date.toDate();
    }

    DateTime toDateTime() const
    {
        return DateTime(date.toDate(), time.toTimeOfDay());
    }

    void toString(W)(ref W writer) const
    {
        date.toString(writer);
        writer.put(' ');
        time.toString(writer);
    }
}

private void skip(ref const(char)[] x, char ch)
{
    if (x.length && (x.ptr[0] == ch))
    {
        x = x[1..$];
    }
    else
    {
        throw new PgSQLProtocolException("Bad datetime string format");
    }
}

private void skipFront(ref const(char)[] x)
{
    if (x.length)
    {
        x = x[1..$];
    }
    else
    {
        throw new PgSQLProtocolException("Bad datetime string format");
    }
}

auto parsePgSQLDate(ref const(char)[] x)
{
    auto year = x.parse!ushort;
    x.skip('-');
    auto month = x.parse!ubyte;
    x.skip('-');
    auto day = x.parse!ubyte;
    return PgSQLDate(year, month, day);
}

auto parsePgSQLTime(ref const(char)[] x)
{
    auto hour = x.parse!ubyte;
    x.skip(':');
    auto minute = x.parse!ubyte;
    x.skip(':');
    auto second = x.parse!ubyte;
    uint usecs;

    if (x.length && (*x.ptr == '.'))
    {
        x.skipFront();

        const len = x.length;
        const frac = x.parse!uint;
        switch (len - x.length) {
        case 1: usecs = frac * 100_000; break;
        case 2: usecs = frac * 10_000; break;
        case 3: usecs = frac * 1_000; break;
        case 4: usecs = frac * 100; break;
        case 5: usecs = frac * 10; break;
        case 6: break;
        default: throw new PgSQLProtocolException("Bad datetime string format");
        }
    }

    byte hoffset;
    byte moffset;

    if (x.length)
    {
        auto sign = *x.ptr == '-' ? -1 : 1;
        x.skipFront();

        hoffset = cast(byte)(sign * x.parse!ubyte);
        if (x.length)
        {
            x.skip(':');
            moffset = cast(byte)(sign * x.parse!ubyte);
        }
    }

    return PgSQLTime(usecs, hour, minute, second, hoffset, moffset);
}

auto parsePgSQLTimestamp(ref const(char)[] x)
{
    auto date = parsePgSQLDate(x);
    x.skipFront();
    auto time = parsePgSQLTime(x);

    return PgSQLTimestamp(date, time);
}

void eatValueText(ref InputPacket packet, ref const PgSQLColumn column, ref PgSQLValue value)
{
    auto length = packet.eat!uint;
    if (length != uint.max)
    {
        auto svalue = packet.eat!(const(char)[])(length);

        final switch(column.type) with (PgColumnTypes)
        {
        case UNKNOWN:
        case NULL:
            value = PgSQLValue(column.name, column.type, null, 0);
            break;
        case BOOL:
            auto x = *svalue.ptr == 't';
            value = PgSQLValue(column.name, column.type, &x, 1);
            break;
        case CHAR:
            value = PgSQLValue(column.name, column.type, cast(void*)svalue.ptr, 1);
            break;
        case INT2:
            auto x = svalue.to!short;
            value = PgSQLValue(column.name, column.type, &x, 2);
            break;
        case INT4:
            auto x = svalue.to!int;
            value = PgSQLValue(column.name, column.type, &x, 4);
            break;
        case INT8:
            auto x = svalue.to!long;
            value = PgSQLValue(column.name, column.type, &x, 8);
            break;
        case REAL:
            auto x = svalue.to!float;
            value = PgSQLValue(column.name, column.type, &x, 4);
            break;
        case DOUBLE:
            auto x = svalue.to!double;
            value = PgSQLValue(column.name, column.type, &x, 8);
            break;
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
        case BYTEA:
        case VARCHAR:
        case CHARA:
            value = PgSQLValue(column.name, column.type, &svalue, typeof(svalue).sizeof);
            break;

        case DATE:
            auto x = parsePgSQLDate(svalue);
            value = PgSQLValue(column.name, column.type, &x, x.sizeof);
            break;
        case TIME:
        case TIMETZ:
            auto x = parsePgSQLTime(svalue);
            value = PgSQLValue(column.name, column.type, &x, x.sizeof);
            break;
        case TIMESTAMP:
        case TIMESTAMPTZ:
            auto x = parsePgSQLTimestamp(svalue);
            value = PgSQLValue(column.name, column.type, &x, x.sizeof);
            break;
        case INTERVAL:
        case JSONB:
            break;
        }
    }
    else
    {
        value = PgSQLValue(column.name, PgColumnTypes.NULL, null, 0);
    }
}

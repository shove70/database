module postgresql.packet;

import std.algorithm;
import std.bitmanip;
import std.traits;

import postgresql.exception;

pragma(inline, true) T host(T)(T x) if (isScalarType!T)
{
    static if (T.sizeof > 1)
    {
        return *cast(T*)(nativeToBigEndian(x).ptr);
    }
    else
    {
        return x;
    }
}

pragma(inline, true) T native(T)(T x) if (isScalarType!T)
{
    static if (T.sizeof > 1)
    {
        return bigEndianToNative!T(*cast(ubyte[T.sizeof]*)&x);
    }
    else
    {
        return x;
    }
}

pragma(inline, true) T native(T)(ubyte* ptr) if (isScalarType!T)
{
    static if (T.sizeof > 1)
    {
        return bigEndianToNative!T(*cast(ubyte[T.sizeof]*)ptr);
    }
    else
    {
        return x.ptr[0];
    }
}

struct InputPacket
{
    @disable this();

    this(ubyte type, ubyte[]* buffer)
    {
        type_ = type;
        buffer_ = buffer;
        in_ = *buffer_;
    }

    ubyte type() const
    {
        return type_;
    }

    T peek(T)() if (!isArray!T)
    {
        assert(T.sizeof <= in_.length);
        return native(*(cast(T*)in_.ptr));
    }

    T eat(T)() if (!isArray!T)
    {
        assert(T.sizeof <= in_.length);
        auto ptr = cast(T*)in_.ptr;
        in_ = in_[T.sizeof..$];
        return native(*ptr);
    }

    const(char)[] peekz()
    {
        import core.stdc.string : strlen;
        return cast(const(char)[])in_[0..strlen(cast(char*)in_.ptr)];
    }

    const(char)[] eatz()
    {
        import core.stdc.string : strlen;
        auto len = strlen(cast(char*)in_.ptr);
        auto result = cast(const(char)[])in_[0..len];
        in_ = in_[len + 1..$];
        return result;
    }

    void skipz()
    {
        import core.stdc.string : strlen;
        auto len = strlen(cast(char*)in_.ptr);
        in_ = in_[len + 1..$];
    }

    T eat(T)(size_t count) if (isArray!T)
    {
        alias ValueType = typeof(T.init[0]);

        assert(ValueType.sizeof * count <= in_.length);
        auto ptr = cast(ValueType*)in_.ptr;
        in_ = in_[ValueType.sizeof * count..$];
        return ptr[0..count];
    }

    void expect(T)(T x)
    {
        if (x != eat!T)
            throw new PgSQLProtocolException("Bad packet format");
    }

    void skip(size_t count)
    {
        assert(count <= in_.length);
        in_ = in_[count..$];
    }

    auto countUntil(ubyte x, bool expect)
    {
        auto index = in_.countUntil(x);
        if (expect)
        {
            if ((index < 0) || (in_[index] != x))
                throw new PgSQLProtocolException("Bad packet format");
        }
        return index;
    }

    void skipLenEnc()
    {
        auto header = eat!ubyte;
        if (header >= 0xfb)
        {
            switch(header)
            {
                case 0xfb:
                    return;
                case 0xfc:
                    skip(2);
                    return;
                case 0xfd:
                    skip(3);
                    return;
                case 0xfe:
                    skip(8);
                    return;
                default:
                    throw new PgSQLProtocolException("Bad packet format");
            }
        }
    }

    ulong eatLenEnc()
    {
        auto header = eat!ubyte;
        if (header < 0xfb)
            return header;

        ulong lo;
        ulong hi;

        switch(header)
        {
        case 0xfb:
            return 0;
        case 0xfc:
            return eat!ushort;
        case 0xfd:
            lo = eat!ubyte;
            hi = eat!ushort;
            return lo | (hi << 8);
        case 0xfe:
            lo = eat!uint;
            hi = eat!uint;
            return lo | (hi << 32);
        default:
            throw new PgSQLProtocolException("Bad packet format");
        }
    }

    auto get() const
    {
        return in_;
    }

    auto remaining() const
    {
        return in_.length;
    }

    bool empty() const
    {
        return in_.length == 0;
    }

protected:

    ubyte[]* buffer_;
    ubyte[] in_;
    ubyte type_;
}

struct OutputPacket
{
    @disable this();

    this(ubyte[]* buffer)
    {
        buffer_ = buffer;
        implicit_ = 4;
        out_ = buffer_.ptr + 4;
    }

    this(ubyte type, ubyte[]* buffer)
    {
        buffer_ = buffer;
        implicit_ = 5;
        if (buffer_.length < implicit_)
            buffer_.length = implicit_;
        *buffer_.ptr = type;
        out_ = buffer_.ptr + implicit_;
    }

    void putz(const(char)[] x)
    {
        put(x);
        put!ubyte(0);
    }

    pragma(inline, true) void put(T)(T x)
    {
        put(offset_, x);
    }

    pragma(inline, true) void put(T)(size_t offset, T x) if (!isArray!T)
    {
        grow(offset, T.sizeof);

        *(cast(T*)(out_ + offset)) = host(x);
        offset_ = max(offset + T.sizeof, offset_);
    }

    void put(T)(size_t offset, T x) if (isArray!T)
    {
        alias ValueType = Unqual!(typeof(T.init[0]));

        grow(offset, ValueType.sizeof * x.length);

        static if (ValueType.sizeof == 1)
        {
            (cast(ValueType*)(out_ + offset))[0..x.length] = x;
        }
        else
        {
            auto pout = cast(ValueType*)(out_ + offset);
            foreach (ref y; x)
                *pout++ = host(y);
        }
        offset_ = max(offset + (ValueType.sizeof * x.length), offset_);
    }

    void putLenEnc(ulong x)
    {
        if (x < 0xfb)
        {
            put!ubyte(cast(ubyte)x);
        }
        else if (x <= ushort.max)
        {
            put!ubyte(0xfc);
            put!ushort(cast(ushort)x);
        }
        else if (x <= (uint.max >> 8))
        {
            put!ubyte(0xfd);
            put!ubyte(cast(ubyte)(x));
            put!ushort(cast(ushort)(x >> 8));
        }
        else
        {
            put!ubyte(0xfe);
            put!uint(cast(uint)x);
            put!uint(cast(uint)(x >> 32));
        }
    }

    size_t marker(T)() if (!isArray!T)
    {
        grow(offset_, T.sizeof);

        auto place = offset_;
        offset_ += T.sizeof;
        return place;
    }

    size_t marker(T)(size_t count) if (isArray!T)
    {
        alias ValueType = Unqual!(typeof(T.init[0]));
        grow(offset_, ValueType.sizeof * x.length);

        auto place = offset_;
        offset_ += (ValueType.sizeof * x.length);
        return place;
    }

    void finalize()
    {
        if ((offset_ + implicit_) > int.max)
            throw new PgSQLConnectionException("Packet size exceeds 2^31");
        *(cast(uint*)(buffer_.ptr + implicit_ - 4)) = host(cast(uint)(4 + offset_));
    }

    void finalize(ubyte)
    {
        finalize();
    }

    void finalize(ubyte seq, size_t extra)
    {
        finalize();
    }

    void reset()
    {
        offset_ = 0;
    }

    void reserve(size_t size)
    {
        (*buffer_).length = max((*buffer_).length, implicit_ + size);
        out_ = buffer_.ptr + implicit_;
    }

    void fill(ubyte x, size_t size)
    {
        grow(offset_, size);
        out_[offset_..offset_ + size] = 0;
        offset_ += size;
    }

    size_t length() const
    {
        return offset_;
    }

    bool empty() const
    {
        return offset_ == 0;
    }

    const(ubyte)[] get() const
    {
        return (*buffer_)[0..implicit_ + offset_];
    }

protected:

    void grow(size_t offset, size_t size)
    {
        auto requested = implicit_ + offset + size;
        if (requested > buffer_.length)
        {
            auto capacity = max(128, (*buffer_).capacity);
            while (capacity < requested)
                capacity <<= 1;
            buffer_.length = capacity;
            out_ = buffer_.ptr + implicit_;
        }
    }

    ubyte[]* buffer_;

    ubyte* out_;
    size_t offset_;
    size_t implicit_;
}

module database.mysql.packet;

import std.algorithm;
import std.traits;
import database.mysql.exception;
import database.util;

struct InputPacket
{
	@disable this();

	this(ubyte[]* buffer)
	{
		buf = buffer;
		in_ = *buf;
	}

	T peek(T)() if (!isArray!T)
	{
		assert(T.sizeof <= in_.length);
		return *(cast(T*)in_.ptr);
	}

	T eat(T)() if (!isArray!T)
	{
		assert(T.sizeof <= in_.length);
		auto ptr = cast(T*)in_.ptr;
		in_ = in_[T.sizeof..$];
		return *ptr;
	}

	T peek(T)(size_t count) if (isArray!T)
	{
		alias ValueType = typeof(Type.init[0]);

		assert(ValueType.sizeof * count <= in_.length);
		auto ptr = cast(ValueType*)in_.ptr;
		return ptr[0..count];
	}

	T eat(T)(size_t count) if (isArray!T)
	{
		alias ValueType = typeof(T.init[0]);

		assert(ValueType.sizeof * count <= in_.length);
		auto ptr = cast(ValueType*)in_.ptr;
		in_ = in_[ValueType.sizeof * count..$];
		return ptr[0..count];
	}

	mixin InputPacketMethods!MySQLProtocolException;

private:
	ubyte[]* buf;
	ubyte[] in_;
}

struct OutputPacket
{
	@disable this();

	this(ubyte[]* buffer)
	{
		buf = buffer;
		out_ = buf.ptr + 4;
	}

	pragma(inline, true) void put(T)(T x)
	{
		put(pos, x);
	}

	void put(T)(size_t offset, T x) if (!isArray!T)
	{
		grow(offset, T.sizeof);

		*(cast(T*)(out_ + offset)) = x;
		pos = max(offset + T.sizeof, pos);
	}

	void put(T)(size_t offset, T x) if (isArray!T)
	{
		alias ValueType = Unqual!(typeof(T.init[0]));

		grow(offset, ValueType.sizeof * x.length);

		(cast(ValueType*)(out_ + offset))[0..x.length] = x;
		pos = max(offset + (ValueType.sizeof * x.length), pos);
	}

	size_t marker(T)() if (!isArray!T)
	{
		grow(pos, T.sizeof);

		auto place = pos;
		pos += T.sizeof;
		return place;
	}

	size_t marker(T)() if (isArray!T)
	{
		alias ValueType = Unqual!(typeof(T.init[0]));
		grow(pos, ValueType.sizeof * x.length);

		auto place = pos;
		pos += ValueType.sizeof * x.length;
		return place;
	}

	void finalize(ubyte seq)
	{
		if (pos >= 0xffffff)
			throw new MySQLConnectionException("Packet size exceeds 2^24");
		uint length = cast(uint)pos;
		uint header = cast(uint)((pos & 0xffffff) | (seq << 24));
		*(cast(uint*)buf.ptr) = header;
	}

	void finalize(ubyte seq, size_t extra)
	{
		if (pos + extra >= 0xffffff)
			throw new MySQLConnectionException("Packet size exceeds 2^24");
		uint length = cast(uint)(pos + extra);
		uint header = cast(uint)((length & 0xffffff) | (seq << 24));
		*(cast(uint*)buf.ptr) = header;
	}

	void reserve(size_t size)
	{
		(*buf).length = max((*buf).length, 4 + size);
		out_ = buf.ptr + 4;
	}

	const(ubyte)[] get() const
	{
		return (*buf)[0..4 + pos];
	}

	void fill(size_t size) @trusted {
		static if (is(typeof(grow)))
			grow(pos, size);
		out_[pos .. pos + size] = 0;
		pos += size;
	}

	mixin OutputPacketMethods;

private:
	void grow(size_t offset, size_t size)
	{
		auto requested = 4 + offset + size;

		if (requested > buf.length)
		{
			auto capacity = max(128, (*buf).capacity);
			while (capacity < requested)
			{
				capacity <<= 1;
			}

			buf.length = capacity;
			out_ = buf.ptr + 4;
		}
	}

	ubyte[]* buf;
	ubyte* out_;
	size_t pos;
}

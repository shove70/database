module database.postgresql.packet;

import std.algorithm;
import std.bitmanip;
import std.traits;
import database.postgresql.exception;
import database.util;
import core.stdc.string : strlen;

struct InputPacket {
	@disable this();

	this(ubyte type, ubyte[] buffer) {
		typ = type;
		in_ = buffer;
	}

	@property ubyte type() const { return typ; }

	T peek(T)() if (!isArray!T) in(T.sizeof <= in_.length) {
		return native(*(cast(T*)in_.ptr));
	}

	T eat(T)() if (!isArray!T) in(T.sizeof <= in_.length) {
		auto ptr = cast(T*)in_.ptr;
		in_ = in_[T.sizeof..$];
		return native(*ptr);
	}

	string peekz() {
		return cast(string)in_[0..strlen(cast(char*)in_.ptr)];
	}

	string eatz() {
		auto len = strlen(cast(char*)in_.ptr);
		auto result = cast(string)in_[0..len];
		in_ = in_[len + 1..$];
		return result;
	}

	void skipz() {
		auto len = strlen(cast(char*)in_.ptr);
		in_ = in_[len + 1..$];
	}

	T eat(T)(size_t count) if (isArray!T) {
		alias ValueType = typeof(T.init[0]);

		assert(ValueType.sizeof * count <= in_.length);
		auto ptr = cast(ValueType*)in_.ptr;
		in_ = in_[ValueType.sizeof * count..$];
		return ptr[0..count];
	}

	mixin InputPacketMethods!PgSQLProtocolException;

private:
	ubyte[] in_;
	ubyte typ;
}

struct OutputPacket {
	@disable this();

	this(ubyte[]* buffer) {
		buf = buffer;
		implicit = 4;
		out_ = buf.ptr + 4;
	}

	this(ubyte type, ubyte[]* buffer) {
		buf = buffer;
		implicit = 5;
		if (buf.length < implicit)
			buf.length = implicit;
		*buf.ptr = type;
		out_ = buf.ptr + implicit;
	}

	void putz(string x) {
		put(x);
		put!ubyte(0);
	}

	void put(T)(T x) {
		put(pos, x);
	}

	pragma(inline, true) void put(T)(size_t offset, T x) if (!isArray!T) {
		grow(offset, T.sizeof);

		*(cast(T*)(out_ + offset)) = native(x);
		pos = max(offset + T.sizeof, pos);
	}

	void put(T)(size_t offset, T x) if (isArray!T) {
		alias ValueType = Unqual!(typeof(T.init[0]));

		grow(offset, ValueType.sizeof * x.length);

		static if (ValueType.sizeof == 1)
			(cast(ValueType*)(out_ + offset))[0..x.length] = x;
		else {
			auto pout = cast(ValueType*)(out_ + offset);
			foreach (ref y; x)
				*pout++ = native(y);
		}
		pos = max(offset + (ValueType.sizeof * x.length), pos);
	}

	void finalize() {
		if (pos + implicit > int.max)
			throw new PgSQLConnectionException("Packet size exceeds 2^31");
		uint p = cast(uint)pos + 4;
		*(cast(uint*)(buf.ptr + implicit - 4)) = native(p);
	}

	void reserve(size_t size) {
		(*buf).length = max((*buf).length, implicit + size);
		out_ = buf.ptr + implicit;
	}

	const(ubyte)[] get() const
	{
		return (*buf)[0..implicit + pos];
	}

	mixin OutputPacketMethods;

private:
	void grow(size_t offset, size_t size) {
		auto requested = implicit + offset + size;
		if (requested > buf.length) {
			auto capacity = max(128, (*buf).capacity);
			while (capacity < requested)
				capacity <<= 1;
			buf.length = capacity;
			out_ = buf.ptr + implicit;
		}
	}

	ubyte[]* buf;

	ubyte* out_;
	size_t pos, implicit;
}

T native(T)(ref T x) {
	return native!T(&x);
}

T native(T)(const T* ptr) if (isScalarType!T) {
	import core.bitop;

	version (LittleEndian)
		enum LE = true;
	else
		enum LE = false;
	static if (T.sizeof > 1 && LE)
		static if (T.sizeof == 2)
			return byteswap(*ptr);
		else static if (T.sizeof == 4)
			return bswap(*cast(uint*)ptr);
		else
			return bswap(*cast(ulong*)ptr);
	else
		return *ptr;
}

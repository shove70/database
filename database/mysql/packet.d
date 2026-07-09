module database.mysql.packet;

import std.algorithm;
import std.traits;
import database.mysql.exception;
import database.util;

struct InputPacket {
	/++ Read-only wrapper around a protocol packet receive buffer. +/
	@disable this();

	/++ Wrap a byte buffer for typed packet reads. +/
	this(ubyte[]* buffer) {
		buf = *buffer;
	}

	/++ Read one scalar value without consuming it. +/
	T peek(T)() if (!isArray!T) {
		assert(T.sizeof <= buf.length);
		return *(cast(T*)buf.ptr);
	}

	/++ Read one scalar value and consume it from the buffer. +/
	T eat(T)() if (!isArray!T) {
		assert(T.sizeof <= buf.length);
		auto ptr = cast(T*)buf.ptr;
		buf = buf[T.sizeof .. $];
		return *ptr;
	}

	/++ Read `count` elements from the buffer as an array slice. +/
	T peek(T)(size_t count) if (isArray!T) {
		alias ValueType = typeof(Type.init[0]);

		assert(ValueType.sizeof * count <= buf.length);
		auto ptr = cast(ValueType*)buf.ptr;
		return ptr[0 .. count];
	}

	/++ Read and consume `count` elements from the buffer as an array slice. +/
	T eat(T)(size_t count) if (isArray!T) {
		alias ValueType = typeof(T.init[0]);

		assert(ValueType.sizeof * count <= buf.length);
		auto ptr = cast(ValueType*)buf.ptr;
		buf = buf[ValueType.sizeof * count .. $];
		return ptr[0 .. count];
	}

	mixin InputPacketMethods!MySQLProtocolException;

private:
	ubyte[] buf;
}

struct OutputPacket {
	/++ Builder for protocol packets to send over the socket. +/
	@disable this();
	@disable this(this);

	/++ Attach a mutable output buffer for payload construction. +/
	this(ubyte[]* buffer) {
		buf = buffer;
		out_ = buf.ptr + 4;
	}

	/++ Append a scalar value at the current write position. +/
	pragma(inline, true) void put(T)(T x) {
		put(pos, x);
	}

	/++ Write a scalar value at an explicit packet offset. +/
	void put(T)(size_t offset, T x) if (!isArray!T) {
		grow(offset, T.sizeof);

		*(cast(T*)(out_ + offset)) = x;
		pos = max(offset + T.sizeof, pos);
	}

	/++ Write an array payload at an explicit packet offset. +/
	void put(T)(size_t offset, T x) if (isArray!T) {
		alias ValueType = Unqual!(typeof(T.init[0]));

		grow(offset, ValueType.sizeof * x.length);

		(cast(ValueType*)(out_ + offset))[0 .. x.length] = x;
		pos = max(offset + (ValueType.sizeof * x.length), pos);
	}

	/++ Reserve a marker offset for a scalar write and advance position. +/
	size_t marker(T)() if (!isArray!T) {
		grow(pos, T.sizeof);

		auto place = pos;
		pos += T.sizeof;
		return place;
	}

	/++ Reserve a marker offset for an array write and advance position. +/
	size_t marker(T)() if (isArray!T) {
		alias ValueType = Unqual!(typeof(T.init[0]));
		grow(pos, ValueType.sizeof * x.length);

		auto place = pos;
		pos += ValueType.sizeof * x.length;
		return place;
	}

	/++ Finalize packet length and sequence number. +/
	void finalize(ubyte seq) {
		if (pos >= 0xffffff)
			throw new MySQLConnectionException("Packet size exceeds 2^24");
		uint header = cast(uint)((pos & 0xffffff) | (seq << 24));
		*(cast(uint*)buf.ptr) = header;
	}

	/++ Finalize packet length with extra payload and sequence number. +/
	void finalize(ubyte seq, size_t extra) {
		if (pos + extra >= 0xffffff)
			throw new MySQLConnectionException("Packet size exceeds 2^24");
		uint length = cast(uint)(pos + extra);
		uint header = cast(uint)((length & 0xffffff) | (seq << 24));
		*(cast(uint*)buf.ptr) = header;
	}

	/++ Ensure the packet has room for at least `size` bytes. +/
	void reserve(size_t size) {
		(*buf).length = max((*buf).length, 4 + size);
		out_ = buf.ptr + 4;
	}

	/++ Get the active packet bytes including header. +/
	const(ubyte)[] get() const
		=> (*buf)[0 .. 4 + pos];

	/++ Fill the next `size` bytes with zeros. +/
	void fill(size_t size) @trusted {
		static if (is(typeof(grow)))
			grow(pos, size);
		out_[pos .. pos + size] = 0;
		pos += size;
	}

	mixin OutputPacketMethods;

private:
	void grow(size_t offset, size_t size) {
		auto requested = 4 + offset + size;

		if (requested > buf.length) {
			auto capacity = max(128, (*buf).capacity);
			while (capacity < requested)
				capacity <<= 1;

			buf.length = capacity;
			out_ = buf.ptr + 4;
		}
	}

	ubyte[]* buf;
	ubyte* out_;
	size_t pos;
}

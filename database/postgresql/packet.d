module database.postgresql.packet;

import std.algorithm;
import std.datetime;
import std.traits;
import database.postgresql.protocol;
import database.util;
import core.stdc.string : strlen;

package import database.postgresql.exception;

@safe:

struct InputPacket {
	@disable this();

	this(ubyte type, ubyte[] buffer) {
		typ = type;
		in_ = buffer;
	}

	@property auto type() const {
		return typ;
	}

	T peek(T)() const if (!is(T == struct) && !isArray!T)
	in (T.sizeof <= in_.length) {
		return native(cast(const T*)in_.ptr);
	}

	T eat(T)() @trusted if (!is(T == struct) && !isArray!T)
	in (T.sizeof <= in_.length) {
		auto p = *cast(T*)in_.ptr;
		in_ = in_[T.sizeof .. $];
		return native(p);
	}

	T eat(T : Date)() {
		return PGEpochDate + dur!"days"(eat!int);
	}

	T eat(T : TimeOfDay)() {
		return PGEpochTime + dur!"usecs"(eat!long);
	}

	T eat(T : DateTime)() { // timestamp
		return PGEpochDateTime + dur!"usecs"(eat!long);
	}

	T eat(T : SysTime)() { // timestamptz
		T x = T(PGEpochDateTime + dur!"usecs"(eat!long), UTC());
		x.timezone = null;
		return x;
	}

	auto eatz() @trusted {
		auto len = strlen(cast(char*)in_.ptr);
		auto result = cast(char[])in_[0 .. len];
		in_ = in_[len + 1 .. $];
		return result;
	}

	T eat(T)(size_t count) @trusted if (isDynamicArray!T) {
		alias ValueType = typeof(T.init[0]);

		assert(ValueType.sizeof * count <= in_.length);
		auto ptr = cast(ValueType*)in_.ptr;
		in_ = in_[ValueType.sizeof * count .. $];
		return ptr[0 .. count];
	}

	mixin InputPacketMethods!PgSQLProtocolException;

private:
	ubyte[] in_;
	ubyte typ;
}

struct OutputPacket {
	@disable this();

	this(ref ubyte[] buffer) @trusted {
		buf = &buffer;
		implicit = 4;
		out_ = &buffer[4];
	}

	this(ubyte type, ref ubyte[] buffer) @trusted {
		buf = &buffer;
		implicit = 5;
		if (buf.length < 5)
			buf.length = 5;
		buffer[0] = type;
		out_ = &buffer[5];
	}

	void put(in char[] x) {
		put(pos, cast(const ubyte[])x);
		put!ubyte(0);
	}

	void put(T)(T x) if (!is(T == struct) && !is(T : const char[])) {
		put(pos, x);
	}

	void put(Date x) {
		put(cast(int)(x.dayOfGregorianCal - PGEpochDay));
	}

	void put(in TimeOfDay x) {
		put(cast(int)(x - PGEpochTime).total!"usecs");
	}

	void put(in DateTime x) { // timestamp
		put(cast(int)(x - PGEpochDateTime).total!"usecs");
	}

	void put(in SysTime x) { // timestamptz
		put(cast(int)(x - SysTime(PGEpochDateTime, UTC())).total!"usecs");
	}

	ubyte[] data() {
		finalize();
		return (*buf)[0 .. implicit + pos];
	}

	mixin OutputPacketMethods;

private:
	pragma(inline, true) void put(T)(size_t offset, T x) @trusted if (!isArray!T)
	out (; pos <= buf.length) {
		grow(offset, T.sizeof);
		*cast(T*)(out_ + offset) = native(x);
		pos = max(offset + T.sizeof, pos);
	}

	void put(T)(size_t offset, T x) @trusted if (isArray!T) {
		alias ValueType = Unqual!(typeof(T.init[0]));

		grow(offset, ValueType.sizeof * x.length);
		static if (ValueType.sizeof == 1)
			(cast(ValueType*)(out_ + offset))[0 .. x.length] = x;
		else {
			auto pout = cast(ValueType*)(out_ + offset);
			foreach (ref y; x)
				*pout++ = native(y);
		}
		pos = max(offset + ValueType.sizeof * x.length, pos);
	}

	void grow(size_t offset, size_t size) @trusted {
		auto requested = implicit + offset + size;
		if (requested > buf.length) {
			auto capacity = max(128, (*buf).capacity);
			while (capacity < requested)
				capacity <<= 1;
			buf.length = capacity;
			out_ = buf.ptr + implicit;
		}
	}

	void finalize() @trusted {
		if (pos + implicit > int.max)
			throw new PgSQLConnectionException("Packet size exceeds 2^31");
		*cast(uint*)(buf.ptr + implicit - 4) = native(cast(uint)pos + 4);
	}

	ubyte[]* buf;
	ubyte* out_;
	size_t pos, implicit;
}

T native(T)(in T x) @trusted if (isScalarType!(OriginalType!T)) {
	import core.bitop;

	version (LittleEndian)
		enum LE = true;
	else
		enum LE = false;
	static if (T.sizeof > 1 && LE)
		static if (T.sizeof == 2)
			return cast(T)byteswap(x);
		else static if (T.sizeof == 4)
			return cast(T)bswap(*cast(const uint*)&x);
		else
			return cast(T)bswap(*cast(const ulong*)&x);
	else
		return x;
}

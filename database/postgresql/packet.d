module database.postgresql.packet;

import core.stdc.stdlib,
database.postgresql.protocol,
database.util,
std.algorithm,
std.datetime,
std.meta,
std.traits;

package import database.postgresql.exception;

@safe:

struct InputPacket {
	@disable this();

	this(ubyte type, ubyte[] buffer) {
		typ = type;
		in_ = buffer;
	}

	@property auto type() const => typ;

	T peek(T)() const if (!is(T == struct) && !isArray!T)
	in (T.sizeof <= in_.length) => native(cast(const T*)in_.ptr);

	T eat(T)() @trusted if (!is(T == struct) && !isArray!T)
	in (T.sizeof <= in_.length) {
		auto p = *cast(T*)in_.ptr;
		in_ = in_[T.sizeof .. $];
		return native(p);
	}

	T eat(T : Date)() => PGEpochDate + dur!"days"(eat!int);

	T eat(T : TimeOfDay)() => PGEpochTime + dur!"usecs"(eat!long);

	T eat(T : DateTime)() // timestamp
	=> PGEpochDateTime + dur!"usecs"(eat!long);

	T eat(T : SysTime)() // timestamptz
	=> T(PGEpochDateTime + dur!"usecs"(eat!long));

	auto eatz() @trusted {
		import core.stdc.string;

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
	@disable this(this);

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

	void put(T)(T x) if (!is(T == struct) && !is(T : const char[]))
		=> put(pos, x);

	void put(Date x)
		=> put(x.dayOfGregorianCal - PGEpochDay);

	void put(TimeOfDay x)
		=> put(cast(int)(x - PGEpochTime).total!"usecs");

	void put(DateTime x) // timestamp
	=> put(cast(int)(x - PGEpochDateTime).total!"usecs");

	void put(in SysTime x) // timestamptz
	=> put(cast(int)(x - SysTime(PGEpochDateTime, UTC())).total!"usecs");

	ubyte[] data() @trusted {
		if (pos + implicit > int.max)
			throw new PgSQLConnectionException("Packet size exceeds 2^31");
		*cast(uint*)(buf.ptr + implicit - 4) = native(cast(uint)pos + 4);
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

	ubyte[]* buf;
	ubyte* out_;
	size_t pos, implicit;
}

package:

version (LittleEndian) {
	import std.bitmanip : native = swapEndian;

	T native(T)(T value) @trusted if (isFloatingPoint!T) {
		union U {
			AliasSeq!(int, long)[T.sizeof / 8] i;
			T f;
		}

		return U(.native(*cast(typeof(U.i)*)&value)).f;
	}
} else
	T native(T)(in T value) => value;

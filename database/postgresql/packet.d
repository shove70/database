module database.postgresql.packet;

import core.stdc.stdlib,
database.postgresql.protocol,
database.util,
std.algorithm,
std.datetime,
std.meta,
std.traits;

package import database.postgresql.exception;

@safe @nogc:

struct InputPacket {
	@disable this();
	@disable this(this);

	this(ubyte type, ubyte[] buffer) {
		typ = type;
		buf = buffer;
	}

	@property auto type() const => typ;

	T peek(T)() @trusted const if (!isAggregateType!T && !isArray!T)
	in (T.sizeof <= buf.length) => native(cast(const T*)buf.ptr);

	T eat(T)() @trusted if (!isAggregateType!T && !isArray!T)
	in (T.sizeof <= buf.length) {
		auto p = *cast(T*)buf.ptr;
		buf = buf[T.sizeof .. $];
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

		auto len = strlen(cast(char*)buf.ptr);
		auto result = cast(char[])buf[0 .. len];
		buf = buf[len + 1 .. $];
		return result;
	}

	T eat(T : U[], U)(size_t count) @trusted {
		assert(U.sizeof * count <= buf.length);
		auto ptr = cast(U*)buf.ptr;
		buf = buf[U.sizeof * count .. $];
		return ptr[0 .. count];
	}

	mixin InputPacketMethods!PgSQLProtocolException;

private:
	ubyte[] buf;
	ubyte typ;
}

struct OutputPacket {
	@disable this();
	@disable this(this);

	this(ubyte[] buffer)
	in (buffer.length >= 4) {
		if (!buffer)
			throw new PgSQLException("Out of memory");
		buf = buffer;
		implicit = 4;
	}

	this(ubyte type, ubyte[] buffer)
	in (buffer.length >= 5) {
		if (!buffer)
			throw new PgSQLException("Out of memory");
		buf = buffer;
		implicit = 5;
		buffer[0] = type;
	}

	~this() @trusted {
		if (buf.length > LargePacketSize)
			free(buf.ptr);
	}

	void put(in char[] x) {
		put(cast(const ubyte[])x);
		put!ubyte(0);
	}

	void put(T)(in T[] x) @trusted if (!is(T[] : const char[])) {
		check(T.sizeof * x.length);
		auto p = cast(T*)(buf.ptr + implicit + pos);
		static if (T.sizeof == 1)
			p[0 .. x.length] = x;
		else {
			foreach (y; x)
				*p++ = native(y);
		}
		pos += cast(uint)(T.sizeof * x.length);
	}

	pragma(inline, true) void put(T)(T x) @trusted
	if (!isAggregateType!T && !isArray!T) {
		check(T.sizeof);
		*cast(Unqual!T*)(buf.ptr + implicit + pos) = native(x);
		pos += T.sizeof;
	}

	void put(Date x)
		=> put(x.dayOfGregorianCal - PGEpochDay);

	void put(TimeOfDay x)
		=> put(cast(int)(x - PGEpochTime).total!"usecs");

	void put(DateTime x) // timestamp
	=> put(cast(int)(x - PGEpochDateTime).total!"usecs");

	void put(in SysTime x) // timestamptz
	=> put(cast(int)(x - SysTime(PGEpochDateTime)).total!"usecs");

	ubyte[] data() @trusted {
		check(0);
		assert(implicit + pos <= buf.length);
		*cast(uint*)(buf.ptr + implicit - 4) = native(pos + 4);
		return buf[0 .. implicit + pos];
	}

	mixin OutputPacketMethods;

private:
	void check(size_t size) {
		if (implicit + pos + size > int.max)
			throw new PgSQLConnectionException("Packet size exceeds 2^31");
	}

	ubyte[] buf;
	uint pos, implicit;
}

package:

enum LargePacketSize = 32 * 1024;

alias IMT = InputMessageType,
OMT = OutputMessageType;

template Output(alias n, Args...) {
	import core.stdc.stdlib;

	auto buf = cast(ubyte*)(n > LargePacketSize ? malloc(n) : alloca(n));
	auto op = OutputPacket(Args, buf[0 .. n]);
}

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

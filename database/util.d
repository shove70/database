module database.util;

// dfmt off
import core.time,
	database.sqlbuilder,
	std.exception,
	std.meta,
	std.socket,
	std.string,
	std.traits,
	std.typecons;
// dfmt on

public import database.traits;

class DBException : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure @safe {
		super(msg, file, line);
	}
}

private:

enum CharClass {
	Other,
	LowerCase,
	UpperCase,
	Underscore,
	Digit
}

CharClass classify(char ch) pure {
	import std.ascii;

	with (CharClass) {
		if (isLower(ch))
			return LowerCase;
		if (isUpper(ch))
			return UpperCase;
		if (isDigit(ch))
			return Digit;
		if (ch == '_')
			return Underscore;
		return Other;
	}
}

public:
S snakeCase(S)(S input, char sep = '_') {
	if (!input.length)
		return "";
	char[128] buffer = void;
	size_t length;

	auto pcls = classify(input[0]);
	foreach (ch; input) {
		auto cls = classify(ch);
		switch (cls) with (CharClass) {
		case UpperCase:
			if (pcls != UpperCase && pcls != Underscore)
				buffer[length++] = sep;
			buffer[length++] = ch | ' ';
			break;
		case Digit:
			if (pcls != Digit)
				buffer[length++] = sep;
			goto default;
		default:
			buffer[length++] = ch;
			break;
		}
		pcls = cls;

		if (length >= buffer.length - 1)
			break;
	}
	return cast(S)buffer[0 .. length].dup;
}

unittest {
	static void test(string str, string expected) {
		auto result = str.snakeCase;
		assert(result == expected, str ~ ": " ~ result);
	}

	test("AA", "aa");
	test("AaA", "aa_a");
	test("AaA1", "aa_a_1");
	test("AaA11", "aa_a_11");
	test("_AaA1", "_aa_a_1");
	test("_AaA11_", "_aa_a_11_");
	test("aaA", "aa_a");
	test("aaAA", "aa_aa");
	test("aaAA1", "aa_aa_1");
	test("aaAA11", "aa_aa_11");
	test("authorName", "author_name");
	test("authorBio", "author_bio");
	test("authorPortraitId", "author_portrait_id");
	test("authorPortraitID", "author_portrait_id");
	test("coverURL", "cover_url");
	test("coverImageURL", "cover_image_url");
}

S camelCase(S, bool upper = false)(in S input, char sep = '_') {
	S output;
	bool upcaseNext = upper;
	foreach (c; input) {
		if (c != sep) {
			if (upcaseNext) {
				output ~= c.toUpper;
				upcaseNext = false;
			} else
				output ~= c.toLower;
		} else
			upcaseNext = true;
	}
	return output;
}

S pascalCase(S)(in S input, char sep = '_')
	=> camelCase!(S, true)(input, sep);

unittest {
	assert("c".camelCase == "c");
	assert("c".pascalCase == "C");
	assert("c_a".camelCase == "cA");
	assert("ca".pascalCase == "Ca");
	assert("camel".pascalCase == "Camel");
	assert("Camel".camelCase == "camel");
	assert("camel_case".pascalCase == "CamelCase");
	assert("camel_camel_case".pascalCase == "CamelCamelCase");
	assert("caMel_caMel_caSe".pascalCase == "CamelCamelCase");
	assert("camel2_camel2_case".pascalCase == "Camel2Camel2Case");
	assert("get_http_response_code".camelCase == "getHttpResponseCode");
	assert("get2_http_response_code".camelCase == "get2HttpResponseCode");
	assert("http_response_code".pascalCase == "HttpResponseCode");
	assert("http_response_code_xyz".pascalCase == "HttpResponseCodeXyz");
}

S quote(S)(S s, char q = '"') if (isSomeString!S) {
	version (NO_SQLQUOTE)
		return s;
	else
		return q ~ s ~ q;
}

S quoteJoin(S, bool leaveTail = false)(S[] s, char sep = ',', char q = '"')
if (isSomeString!S) {
	import std.array;

	auto res = appender!S;
	for (size_t i; i < s.length; i++) {
		version (NO_SQLQUOTE)
			res ~= s[i];
		else {
			res ~= q;
			res ~= s[i];
			res ~= q;
		}
		if (leaveTail || i + 1 < s.length)
			res ~= sep;
	}
	return res[];
}

T parse(T)(inout(char)[] data) if (isIntegral!T)
	=> parse!T(data, 0);

T parse(T)(ref inout(char)[] data, size_t startIndex = 0) if (isIntegral!T)
in (startIndex <= data.length) {
	T x;
	auto i = startIndex;
	for (; i < data.length; ++i) {
		const c = data[i];
		if (c < '0' || c > '9')
			break;
		x = x * 10 + (c ^ '0');
	}
	data = data[i .. $];
	return x;
}

package(database):

auto toStr(T)(T ptr) => fromStringz(ptr).idup;

template InputPacketMethods(E : Exception) {
	void expect(T)(T x) {
		if (x != eat!T)
			throw new E("Bad packet format");
	}

	void skip(size_t count)
	in (count <= buf.length) {
		buf = buf[count .. $];
	}

	auto countUntil(ubyte x, bool expect) {
		auto index = buf.countUntil(x);
		if (expect && (index < 0 || buf[index] != x))
			throw new E("Bad packet format");
		return index;
	}
	// dfmt off
	void skipLenEnc() {
		auto header = eat!ubyte;
		if (header >= 0xfb) {
			switch(header) {
			case 0xfb: return;
			case 0xfc: return skip(2);
			case 0xfd: return skip(3);
			case 0xfe: return skip(8);
			default:
			}
			throw new E("Bad packet format");
		}
	}

	ulong eatLenEnc() {
		auto header = eat!ubyte;
		if (header < 0xfb)
			return header;

		switch(header) {
		case 0xfb: return 0;
		case 0xfc: return eat!ushort;
		case 0xfd:
			_l l = {lo_8: eat!ubyte,
					hi_16: eat!ushort};
			return l.n;
		case 0xfe:
			_l l = {lo: eat!uint,
					hi: eat!uint};
			return l.n;
		default:
		}
		throw new E("Bad packet format");
	}
	// dfmt on

	auto remaining() const => buf.length;

	bool empty() const => buf.length == 0;
}

template OutputPacketMethods() {
	void putLenEnc(ulong x) {
		if (x < 0xfb) {
			put(cast(ubyte)x);
		} else if (x <= ushort.max) {
			put!ubyte(0xfc);
			put(cast(ushort)x);
		} else if (x <= 0xffffff) {
			put!ubyte(0xfd);
			_l l = {n: x};
			put(l.lo_8);
			put(l.hi_16);
		} else {
			put!ubyte(0xfe);
			_l l = {n: x};
			put(l.lo);
			put(l.hi);
		}
	}

	size_t length() const => pos;

	bool empty() const => pos == 0;
}

align(1) union _l {
	struct {
		version (LittleEndian) {
			uint lo;
			uint hi;
		} else {
			uint hi;
			uint lo;
		}
	}

	struct {
		version (LittleEndian) {
			ubyte lo_8;
			ushort hi_16;
		} else {
			byte[5] pad;
			ushort hi_16;
			ubyte lo_8;
		}
	}

	ulong n;
}

class DBSocket(E : Exception) : TcpSocket {
	import core.stdc.errno;

@safe:
	this(in char[] host, ushort port) {
		super(new InternetAddress(host, port));
		setOption(SocketOptionLevel.SOCKET, SocketOption.KEEPALIVE, true);
		setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
		setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, 30.seconds);
		setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, 30.seconds);
	}

	override void close() scope {
		shutdown(SocketShutdown.BOTH);
		super.close();
	}

	void read(void[] buffer) {
		long len = void;

		for (size_t i; i < buffer.length; i += len) {
			len = receive(buffer[i .. $]);

			if (len > 0)
				continue;

			if (len == 0)
				throw new E("Server closed the connection");

			if (errno == EINTR || errno == EAGAIN /* || errno == EWOULDBLOCK*/ )
				len = 0;
			else
				throw new E("Received std.socket.Socket.ERROR: " ~ formatSocketError(errno));
		}
	}

	void write(in void[] buffer) {
		long len = void;

		for (size_t i; i < buffer.length; i += len) {
			len = send(buffer[i .. $]);

			if (len > 0)
				continue;

			if (len == 0)
				throw new E("Server closed the connection");

			if (errno == EINTR || errno == EAGAIN /* || errno == EWOULDBLOCK*/ )
				len = 0;
			else
				throw new E("Sent std.socket.Socket.ERROR: " ~ formatSocketError(errno));
		}
	}
}

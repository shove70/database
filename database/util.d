module database.util;

// dfmt off
import core.time,
	database.sqlbuilder,
	std.exception,
	std.meta,
	std.string,
	std.traits,
	std.typecons;
// dfmt on

public import database.traits;

/++ Base exception for all database-related errors in this project. +/
class DBException : Exception {
	/++ Create a database exception with file and line metadata. +/
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
/++ Convert a string to snake_case.

Params:
	input = The input string.
	sep = The separator to insert between words.

Returns: The snake_case representation of the input string.
+/
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

		if (length >= buffer.length - 1) // @suppress(dscanner.suspicious.length_subtraction)
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

/++ Convert a string to camelCase.

Params:
    upper = Whether to capitalize the first character.
    input = The input string.
    sep = The separator to treat as a word boundary.

Returns: The camelCase representation of the input string.
+/
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

/++ Convert a string to PascalCase.

Params:
    input = The input string.
    sep = The separator to treat as a word boundary.
Returns: The PascalCase representation of the input string.
+/
S pascalCase(S)(in S input, char sep = '_')
	=> camelCase!(S, true)(input, sep);

@safe unittest {
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

/++ Quote a string for SQL.

Params:
	s = The string to quote.
	q = The quote character to use.

Returns: The quoted SQL string.
+/
S quote(S)(S s, char q = '\'')
if (isSomeString!S) {
	import std.algorithm;

	version (NO_SQLQUOTE)
		return s;
	else {
		if (s.canFind(q))
			s = s.replace([q], [q, q]);
		return q ~ s ~ q;
	}
}

@safe unittest {
	assert("a".quote == `'a'`);
	assert("a".quote('\'') == `'a'`);
	assert("a".quote('"') == `"a"`);
	assert(`a"`.quote('"') == `"a"""`);
}

version (NO_SQLQUOTE) {
} else
	immutable sqlKeywords = {
	import std.algorithm;

	bool[string] res;
	foreach (keyword; import("keywords.txt").splitter('\n'))
		res[keyword] = true;
	return res;
}();

S identifier(S)(S s) {
	import std.string;

	version (NO_SQLQUOTE) {
	} else {
		if (s.toUpper in sqlKeywords)
			return '"' ~ s ~ '"';
	}
	return s;
}

S quoteJoin(S, bool leaveTail = false)(S[] s, char sep = ',', char q = '"')
if (isSomeString!S) {
	import std.array;
	import std.string;

	auto res = appender!S;
	for (size_t i; i < s.length; i++) {
		version (NO_SQLQUOTE)
			res ~= s[i];
		else {
			if (s[i].toUpper in sqlKeywords) {
				res ~= q;
				res ~= s[i];
				res ~= q;
			} else
				res ~= s[i];
		}
		if (leaveTail || i + 1 < s.length)
			res ~= sep;
	}
	return res[];
}

@safe unittest {
	assert(quoteJoin!string([]) == "");
	assert(["a", "b"].quoteJoin == `a,b`);
	assert(["group", "on"].quoteJoin(',') == `"group","on"`);
	assert(["group", "on"].quoteJoin(',', '\'') == `'group','on'`);
}

/++ Parse an integral value from a byte slice.

This is an overload set:
1. Parse from the beginning of `data`.
2. Parse from an explicit `startIndex` and advance `data` to the remaining suffix.

Params:
	data = The byte slice containing ASCII digits to parse.
Returns: The parsed integral value.
+/
T parse(T)(inout(char)[] data)
if (isIntegral!T)
	=> parse!T(data, 0);

T parse(T)(ref inout(char)[] data, size_t startIndex = 0)
if (isIntegral!T)
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

/++ Convert a C string pointer into an owning D string copy. +/
auto toStr(T)(T ptr) => fromStringz(ptr).idup;

/++ Packet-reading helpers mixed into packet buffer types. +/
template InputPacketMethods(E : Exception) {
	/++ Ensure the next parsed value matches `x`, otherwise throw protocol error. +/
	void expect(T)(T x) {
		if (x != eat!T)
			throw new E("Bad packet format");
	}

	/++ Skip `count` bytes from the front of the buffer. +/
	void skip(size_t count)
	in (count <= buf.length) {
		buf = buf[count .. $];
	}

	/++ Search bytes until `x` and return index; optionally enforce presence. +/
	auto countUntil(ubyte x, bool expect) {
		auto index = buf.countUntil(x);
		if (expect && (index < 0 || buf[index] != x))
			throw new E("Bad packet format");
		return index;
	}
	// dfmt off
	/++ Skip a length-encoded MySQL value header and payload length. +/
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

	/++ Number of unread bytes currently available in the packet buffer. +/
	auto remaining() const => buf.length;

	/++ True when no unread bytes remain. +/
	bool empty() const => buf.length == 0;
}

/++ Packet-writing helpers mixed into packet output buffers. +/
template OutputPacketMethods() {
	/++ Write a MySQL length-encoded integer to the output buffer. +/
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

	/++ Current output payload length in bytes. +/
	size_t length() const => pos;

	/++ True when no bytes have been written yet. +/
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

template DBSocket(E : Exception) {
	/++ TCP socket wrapper used by database protocol implementations. +/
	import std.socket;
	import core.stdc.errno;
	class DBSocket : TcpSocket {

	@safe:
		/++ Open a TCP socket and configure it for database networking. +/
		this(in char[] host, ushort port) {
			super(new InternetAddress(host, port));
			setOption(SocketOptionLevel.SOCKET, SocketOption.KEEPALIVE, true);
			setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
			setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, 30.seconds);
			setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, 30.seconds);
		}

		/++ Close socket and disable further I/O operations. +/
		override void close() scope {
			shutdown(SocketShutdown.BOTH);
			super.close();
		}

		/++ Read data until the provided buffer is fully filled. +/
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
					throw new E("Received Socket ERROR: " ~ formatSocketError(errno));
			}
		}

		/++ Write all bytes in the provided buffer, handling transient retry errors. +/
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
					throw new E("Sent Socket ERROR: " ~ formatSocketError(errno));
			}
		}
	}
}

module database.util;

public import std.exception : basicExceptionCtors;
import core.stdc.errno;
import core.time;
import std.exception;
import std.socket;
import std.traits;
import std.typecons;

struct as { // @suppress(dscanner.style.phobos_naming_convention)
	string name;
}

enum ignore; // @suppress(dscanner.style.phobos_naming_convention)

enum optional; // @suppress(dscanner.style.phobos_naming_convention)

/// Get the keyname of `T`
template KeyName(alias T, string defaultName = T.stringof) {
	import std.traits;

	static if (hasUDA!(T, ignore))
		enum KeyName = "";
	else static if (hasUDA!(T, as))
		enum KeyName = getUDAs!(T, as)[0].name;
	else
		static foreach (attr; __traits(getAttributes, T))
		static if (is(typeof(KeyName) == void) && is(typeof(attr(""))))
			enum KeyName = attr(defaultName);
	static if (is(typeof(KeyName) == void))
		enum KeyName = defaultName;
}

enum isVisible(alias M) = __traits(getVisibility, M).length == 6; //public or export

private enum fitsInString(T) =
	!isAssociativeArray!T && (!isArray!T || is(typeof(T.init[0]) == ubyte) ||
			is(T == string));

template isWritableDataMember(alias M) {
	alias TM = typeof(M);
	static if (is(AliasSeq!M) || hasUDA!(M, ignore))
		enum isWritableDataMember = false;
	else static if (is(TM == enum))
		enum isWritableDataMember = true;
	else static if (!fitsInString!TM || isSomeFunction!TM)
		enum isWritableDataMember = false;
	else static if (!is(typeof(() { M = TM.init; }())))
		enum isWritableDataMember = false;
	else
		enum isWritableDataMember = isVisible!M;
}

template isReadableDataMember(alias M) {
	alias TM = typeof(M);
	static if (is(AliasSeq!M) || hasUDA!(M, ignore))
		enum isReadableDataMember = false;
	else static if (is(TM == enum))
		enum isReadableDataMember = true;
	else static if (!fitsInString!TM)
		enum isReadableDataMember = false;
	else static if (isSomeFunction!TM /* && return type is valueType*/ )
		enum isReadableDataMember = true;
	else static if (!is(typeof({ TM x = M; })))
		enum isReadableDataMember = false;
	else
		enum isReadableDataMember = isVisible!M;
}

void appendValues(R, T)(ref R appender, T values)
if (isArray!T && !isSomeString!(OriginalType!T)) {
	foreach (i, value; values) {
		appendValue(appender, value);
		if (i != values.length - 1)
			appender.put(',');
	}
}

void appendValue(R, T)(ref R appender, T) if (is(Unqual!T : typeof(null))) {
	appender.put("null");
}

void appendValue(R, T)(ref R appender, T value)
if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T)) {
	appendValue(appender, value.isNull ? null : value.get);
}

@property {
	string placeholders(size_t x, bool parens = true) {
		import std.array;

		if (!x)
			return null;
		auto app = appender!string;
		if (parens) {
			app.reserve((x << 1) - 1);

			app ~= '(';
			foreach (i; 0..x - 1)
				app ~= "?,";
			app ~= '?';
			app ~= ')';
		} else {
			app.reserve(x << 1 | 1);

			foreach (i; 0..x - 1)
				app ~= "?,";
			app ~= '?';
		}
		return app[];
	}

	string placeholders(T)(T x, bool parens = true)
	if (is(typeof(() { auto y = x.length; }))) {
		return x.length.placeholders(parens);
	}
}

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

S camelCase(S, bool upper = false)(S input, char sep = '_') {
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

unittest {
	assert("c".camelCase == "c");
	assert("c".camelCase!true == "C");
	assert("c_a".camelCase == "cA");
	assert("ca".camelCase!true == "Ca");
	assert("camel".camelCase!true == "Camel");
	assert("Camel".camelCase!false == "camel");
	assert("camel_case".camelCase!true == "CamelCase");
	assert("camel_camel_case".camelCase!true == "CamelCamelCase");
	assert("caMel_caMel_caSe".camelCase!true == "CamelCamelCase");
	assert("camel2_camel2_case".camelCase!true == "Camel2Camel2Case");
	assert("get_http_response_code".camelCase == "getHttpResponseCode");
	assert("get2_http_response_code".camelCase == "get2HttpResponseCode");
	assert("http_response_code".camelCase!true == "HttpResponseCode");
	assert("http_response_code_xyz".camelCase!true == "HttpResponseCodeXyz");
}

package(database):

template InputPacketMethods(E : Exception) {
	void expect(T)(T x)
	{
		if (x != eat!T)
			throw new E("Bad packet format");
	}

	void skip(size_t count) in(count <= in_.length) {
		in_ = in_[count..$];
	}

	auto countUntil(ubyte x, bool expect) {
		auto index = in_.countUntil(x);
		if (expect && (index < 0 || in_[index] != x))
			throw new E("Bad packet format");
		return index;
	}

	void skipLenEnc() {
		auto header = eat!ubyte;
		if (header >= 0xfb) {
			switch(header) {
				case 0xfb: return;
				case 0xfc: return skip(2);
				case 0xfd: return skip(3);
				case 0xfe: return skip(8);
				default:
					throw new E("Bad packet format");
			}
		}
	}

	ulong eatLenEnc() {
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
				throw new E("Bad packet format");
		}
	}

	auto remaining() const { return in_.length; }

	bool empty() const { return in_.length == 0; }
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

	void reset() { pos = 0; }

	void fill(ubyte x, size_t size)
	{
		grow(pos, size);
		out_[pos..pos + size] = 0;
		pos += size;
	}

	size_t length() const { return pos; }

	bool empty() const { return pos == 0; }
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

struct DBSocket(E : Exception)
{
	void connect(scope const(char)[] host, ushort port)
	{
		socket = new TcpSocket(new InternetAddress(host, port));
		socket.setOption(SocketOptionLevel.SOCKET, SocketOption.KEEPALIVE, true);
		socket.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
		socket.setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, 30.seconds);
		socket.setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, 30.seconds);
	}

	@property bool connected() inout
	{
		return socket && socket.isAlive();
	}

	void close()
	{
		if (socket)
		{
			socket.shutdown(SocketShutdown.BOTH);
			socket.close();
			socket = null;
		}
	}

	void read(void[] buffer)
	{
		long len;

		for (size_t off; off < buffer.length; off += len)
		{
			len = socket.receive(buffer[off..$]);

			if (len > 0)
				continue;

			if (len == 0)
				throw new E("Server closed the connection");

			if (errno == EINTR || errno == EAGAIN/* || errno == EWOULDBLOCK*/)
				len = 0;
			else

				throw new E("Received std.socket.Socket.ERROR: " ~ formatSocketError(errno));
		}
	}

	void write(in void[] buffer)
	{
		long len;

		for (size_t off; off < buffer.length; off += len)
		{
			len = socket.send(buffer[off..$]);

			if (len > 0)
				continue;

			if (len == 0)
				throw new E("Server closed the connection");

			if (errno == EINTR || errno == EAGAIN/* || errno == EWOULDBLOCK*/)
				len = 0;
			else

				throw new E("Sent std.socket.Socket.ERROR: " ~ formatSocketError(errno));
		}
	}

	private TcpSocket socket;
}

T parse(T)(string data) if (isIntegral!T) {
	return parse!T(data, 0);
}

T parse(T)(ref string data, size_t startIndex = 0) if (isIntegral!T)
in (startIndex <= data.length) {
	T x;
	auto i = startIndex;
	for (; i < data.length; ++i) {
		char c = data[i];
		if (c < '0' || c > '9')
			break;
		x = x * 10 + (c ^ '0');
	}
	data = data[i .. $];
	return x;
}
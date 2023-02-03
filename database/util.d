module database.util;

// dfmt off
import core.time,
	database.sqlbuilder,
	std.exception,
	std.socket,
	std.string,
	std.traits,
	std.typecons;
// dfmt on

class DBException : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure @safe {
		super(msg, file, line);
	}
}

struct as { // @suppress(dscanner.style.phobos_naming_convention)
	string name;
}

enum ignore; // @suppress(dscanner.style.phobos_naming_convention)

enum optional; // @suppress(dscanner.style.phobos_naming_convention)

/// Get the keyname of `T`, return empty if fails
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

enum {
	default0 = "default '0'",
	notnull = "not null",
	unique = "unique"
}

struct sqlkey { // @suppress(dscanner.style.phobos_naming_convention)
	string key;
}

struct sqltype { // @suppress(dscanner.style.phobos_naming_convention)
	string type;
}

/// foreign key
enum foreign(alias field) = sqlkey(ColumnName!(field, true));

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
alias CutOut(size_t I, T...) = AliasSeq!(T[0 .. I], T[I + 1 .. $]);

template getSQLFields(string prefix, string suffix, T) {
	import std.conv : to;
	import std.meta;

	static string putPlaceholders(string[] s) {
		string res;
		for (size_t i; i < s.length;) {
			version (NO_SQLQUOTE)
				res ~= s[i];
			else {
				res ~= '"';
				res ~= s[i];
				res ~= '"';
			}
			++i;
			if (i == s.length)
				res ~= "=$" ~ i.to!string ~ ',';
		}
		return res[];
	}

	enum colNames = ColumnNames!T,
		I = staticIndexOf!("rowid", colNames),
		sql(S...) = prefix ~ (suffix == "=?" ? putPlaceholders([S]) : [S].quoteJoin())
		~ suffix;
	// Skips "rowid" field
	static if (I >= 0)
		enum sqlFields = CutOut!(I, colNames);
	else
		enum sqlFields = colNames;
}

alias toz = toStringz;

auto toStr(T)(T ptr) {
	return fromStringz(ptr).idup;
}

template InputPacketMethods(E : Exception) {
	void expect(T)(T x) {
		if (x != eat!T)
			throw new E("Bad packet format");
	}

	void skip(size_t count)
	in (count <= in_.length) {
		in_ = in_[count .. $];
	}

	auto countUntil(ubyte x, bool expect) {
		auto index = in_.countUntil(x);
		if (expect && (index < 0 || in_[index] != x))
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

	auto remaining() const { return in_.length; }

	bool empty() const { return in_.length == 0; }
	// dfmt on
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

	size_t length() const {
		return pos;
	}

	bool empty() const {
		return pos == 0;
	}
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
	this(scope const(char)[] host, ushort port) {
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

public:
T parse(T)(inout(char)[] data) if (isIntegral!T) {
	return parse!T(data, 0);
}

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

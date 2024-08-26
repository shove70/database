module database.row;

enum Strict {
	yes,
	yesIgnoreNull,
	no,
}

package(database)
struct Row(Value, Header, E:
	Exception, alias hashOf, alias Mixin) {
	import std.algorithm;
	import std.traits;
	import std.conv : text;

	ref auto opDispatch(string key)() const => this[key];

	ref auto opIndex(string key) const {
		if (auto index = find(hashOf(key), key))
			return values[index - 1];
		throw new E("Column '" ~ key ~ "' was not found in this result set");
	}

	ref auto opIndex(size_t index) => values[index];

	const(Value)* opBinaryRight(string op : "in")(string key) pure const {
		if (auto index = find(hashOf(key), key))
			return &values[index - 1];
		return null;
	}

	int opApply(int delegate(const ref Value value) del) const {
		foreach (ref v; values)
			if (auto ret = del(v))
				return ret;
		return 0;
	}

	int opApply(int delegate(ref size_t, const ref Value) del) const {
		foreach (ref i, ref v; values)
			if (auto ret = del(i, v))
				return ret;
		return 0;
	}

	int opApply(int delegate(const ref string, const ref Value) del) const {
		foreach (i, ref v; values)
			if (auto ret = del(_header[i].name, v))
				return ret;
		return 0;
	}

	void toString(R)(ref R app) const {
		app.formattedWrite("%s", values);
	}

	string toString() @trusted const {
		import std.conv : to;

		return values.to!string;
	}

	string[] toStringArray(size_t start = 0, size_t end = size_t.max) const
	in (start <= end) {
		import std.array;

		if (end > values.length)
			end = values.length;
		if (start > values.length)
			start = values.length;

		string[] result = uninitializedArray!(string[])(end - start);
		foreach (i, ref s; result)
			s = values[i].toString();
		return result;
	}

	void get(T, Strict strict = Strict.yesIgnoreNull)(ref T x)
	if (isAggregateType!T) {
		import std.typecons;

		static if (isTuple!T) {
			static if (strict != Strict.no)
				if (x.length >= values.length)
					throw new E(text("Column ", x.length, " is out of range for this result set"));
			foreach (i, ref f; x.tupleof) {
				static if (strict != Strict.yes) {
					if (!this[i].isNull)
						f = this[i].get!(Unqual!(typeof(f)));
				} else
					f = this[i].get!(Unqual!(typeof(f)));
			}
		} else
			structurize!strict(x);
	}

	T get(T)() if (!isAggregateType!T) => this[0].get!T;

	T get(T, Strict strict = Strict.yesIgnoreNull)() if (isAggregateType!T) {
		T result;
		get!(T, strict)(result);
		return result;
	}

	Value[] values;
	alias values this;

	@property Header header() => _header;

package(database):
	@property void header(Header header) {
		_header = header;
		auto headerLen = header.length;
		auto idealLen = headerLen + (headerLen >> 2);
		auto indexLen = index_.length;

		index_[] = 0;

		if (indexLen < idealLen) {
			if (indexLen < 32)
				indexLen = 32;

			while (indexLen < idealLen)
				indexLen <<= 1;

			index_.length = indexLen;
		}

		auto mask = indexLen - 1;
		assert((indexLen & mask) == 0);

		values.length = headerLen;
		foreach (index, ref column; header) {
			auto hash = hashOf(column.name) & mask;
			uint probe;

			for (;;) {
				if (index_[hash] == 0) {
					index_[hash] = cast(uint)index + 1;
					break;
				}

				hash = (hash + ++probe) & mask;
			}
		}
	}

	auto ref get_(size_t index) => values[index]; // TODO

	mixin Mixin;

private:
	Header _header;
	uint[] index_;
}

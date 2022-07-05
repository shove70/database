module database.row;

import std.typecons;

enum Strict
{
	yes = 0,
	yesIgnoreNull,
	no,
}

package(database):
struct Row(Value, Header, E : Exception, alias hashOf, alias Mixin)
{
	import std.algorithm;
	import std.traits;

	@property size_t opDollar() const
	{
		return values_.length;
	}

	@property const(const(char)[])[] columns() const
	{
		return names_;
	}

	@property ref auto opDispatch(string key)() const
	{
		enum hash = hashOf(key);
		return dispatchFast_(hash, key);
	}

	auto opSlice() const
	{
		return values_;
	}

	auto opSlice(size_t i, size_t j) const
	{
		return values_[i..j];
	}

	ref auto opIndex(string key) const
	{
		if (auto index = find_(hashOf(key), key))
			return values_[index - 1];
		throw new E("Column '" ~ key ~ "' was not found in this result set");
	}

	ref auto opIndex(size_t index) const
	{
		return values_[index];
	}

	const(Value)* opBinaryRight(string op)(string key) const if (op == "in")
	{
		if (auto index = find(hashOf(key), key))
			return &values_[index - 1];
		return null;
	}

	int opApply(int delegate(const ref Value value) del) const
	{
		foreach (ref v; values_)
			if (auto ret = del(v))
				return ret;
		return 0;
	}

	int opApply(int delegate(ref size_t, const ref Value) del) const
	{
		foreach (ref size_t i, ref v; values_)
			if (auto ret = del(i, v))
				return ret;
		return 0;
	}

	int opApply(int delegate(const ref const(char)[], const ref Value) del) const
	{
		foreach (size_t i, ref v; values_)
			if (auto ret = del(names_[i], v))
				return ret;
		return 0;
	}

	void toString(Appender)(ref Appender app) const
	{
		import std.format : formattedWrite;
		formattedWrite(&app, "%s", values_);
	}

	string toString() const
	{
		import std.conv : to;
		return to!string(values_);
	}

	string[] toStringArray(size_t start = 0, size_t end = ~cast(size_t)0) const
	{
		end = min(end, values_.length);
		start = min(start, values_.length);
		if (start > end)
			swap(start, end);

		string[] result;
		result.reserve(end - start);
		foreach(i; start..end)
			result ~= values_[i].toString;
		return result;
	}

	string[string] toAA()
	{
		string[string] result;
		foreach(i, name; names_)
		{
			result[name] = values_[i].toString();
		}

		return result;
	}

	void toStruct(T, Strict strict = Strict.yesIgnoreNull)(ref T x) if(is(Unqual!T == struct))
	{
		static if (isTuple!(Unqual!T))
		{
			foreach(i, ref f; x.field)
			{
				if (i < values_.length)
				{
					static if (strict != Strict.yes)
					{
						if (!this[i].isNull)
							f = this[i].get!(Unqual!(typeof(f)));
					}
					else
					{
						f = this[i].get!(Unqual!(typeof(f)));
					}
				}
				else static if ((strict == Strict.yes) || (strict == Strict.yesIgnoreNull))
				{
					throw new E("Column " ~ i ~ " is out of range for this result set");
				}
			}
		}
		else
		{
			structurize!(strict, null)(x);
		}
	}

	void toStruct(Strict strict, T)(ref T x) if (is(Unqual!T == struct)) {
		toStruct!(T, strict)(x);
	}

	T toStruct(T, Strict strict = Strict.yesIgnoreNull)() if (is(Unqual!T == struct))
	{
		T result;
		toStruct!(T, strict)(result);

		return result;
	}

package(database):

	ref auto dispatchFast_(uint hash, string key) const
	{
		if (auto index = find_(hash, key))
			return opIndex(index - 1);
		throw new E("Column '" ~ key ~ "' was not found in this result set");
	}

	void header_(Header header)
	{
		auto headerLen = header.length;
		auto idealLen = (headerLen + (headerLen >> 2));
		auto indexLen = index_.length;

		index_[] = 0;

		if (indexLen < idealLen)
		{
			indexLen = max(32, indexLen);

			while (indexLen < idealLen)
				indexLen <<= 1;

			index_.length = indexLen;
		}

		auto mask = (indexLen - 1);
		assert((indexLen & mask) == 0);

		names_.length = headerLen;
		values_.length = headerLen;
		foreach (index, ref column; header)
		{
			names_[index] = column.name;

			auto hash = hashOf(column.name) & mask;
			auto probe = 1;

			while (true)
			{
				if (index_[hash] == 0)
				{
					index_[hash] = cast(uint)index + 1;
					break;
				}

				hash = (hash + probe++) & mask;
			}
		}
	}

	ref auto get_(size_t index)
	{
		return values_[index];
	}

	mixin Mixin;

private:
	Value[] values_;
	const(char)[][] names_;
	uint[] index_;
}

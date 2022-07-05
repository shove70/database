module database.inserter;

import database.util : appendValues, SQLName = KeyName;
import std.array;
import std.meta;
import std.range;
import std.string;
import std.traits;

enum OnDuplicate : size_t
{
	Ignore,
	Error,
	Replace,
	Update,
	UpdateAll,
}

private {
	enum isSomeStringOrStringArray(T) = isSomeString!(OriginalType!T) || (isArray!T && isSomeString!(ElementType!T));
	enum allStringOrStringArray(T...) = T.length && allSatisfy!(isSomeStringOrStringArray, T);
}

package(database):

template InserterHelpers(Connection, Inserter) {
	auto inserter(ref Connection connection) {
		return Inserter(&connection);
	}

	auto inserter(Args...)(ref Connection connection, OnDuplicate action, string tableName, Args columns) {
		auto insert = Inserter(&connection);
		insert.start(action, tableName, columns);
		return insert;
	}

	auto inserter(Args...)(ref Connection connection, string tableName, Args columns) {
		auto insert = Inserter(&connection);
		insert.start(OnDuplicate.Error, tableName, columns);
		return insert;
	}
}

struct DBInserter(Connection, E : Exception, char quote, alias isValueType, alias appendValue)
{
	@disable this();
	@disable this(this);

	this(Connection* connection)
	{
		conn_ = connection;
		pending_ = 0;
		flushes_ = 0;
	}

	~this()
	{
		flush();
	}

	void start(Args...)(string tableName, Args fieldNames)
	if (allStringOrStringArray!Args) {
		start(OnDuplicate.Error, tableName, fieldNames);
	}

	void start(Args...)(OnDuplicate action, string tableName, Args fieldNames)
	if (allStringOrStringArray!Args) {
		auto fieldCount = fieldNames.length;

		foreach (size_t i, Arg; Args)
		{
			static if (isArray!Arg && !isSomeString!(OriginalType!Arg))
			{
				fieldCount = (fieldCount - 1) + fieldNames[i].length;
			}
		}

		fields_ = fieldCount;

		Appender!(char[]) app;

		final switch(action) with (OnDuplicate)
		{
			case Ignore:
				app.put("insert ignore into ");
				break;
			case Replace:
				app.put("replace into ");
				break;
			case UpdateAll:
				Appender!(char[]) dupapp;

				foreach(size_t i, Arg; Args) {
					static if (isSomeString!(OriginalType!Arg))
					{
						dupapp.put('`');
						dupapp.put(fieldNames[i]);
						dupapp.put("`=values(`");
						dupapp.put(fieldNames[i]);
						dupapp.put("`)");
					}
					else
					{
						auto columns = fieldNames[i];
						foreach (j, name; columns)
						{
							dupapp.put('`');
							dupapp.put(name);
							dupapp.put("`=values(`");
							dupapp.put(name);
							dupapp.put("`)");
							if (j + 1 != columns.length)
								dupapp.put(',');
						}
					}
					if (i + 1 != Args.length)
						dupapp.put(',');
				}
				dupUpdate = dupapp.data;
				goto case;
			case Update:
			case Error:
				app.put("insert into ");
				break;
		}

		app.put(tableName);
		app.put('(');

		foreach (size_t i, Arg; Args)
		{
			static if (isSomeString!(OriginalType!Arg))
			{
				fieldsHash_ ~= hashOf(fieldNames[i]);
				fieldsNames_ ~= fieldNames[i];

				app.put(quote);
				app.put(fieldNames[i]);
				app.put(quote);
			}
			else
			{
				auto columns = fieldNames[i];
				foreach (j, name; columns)
				{
					fieldsHash_ ~= hashOf(name);
					fieldsNames_ ~= name;

					app.put(quote);
					app.put(name);
					app.put(quote);
					if (j + 1 != columns.length)
						app.put(',');
				}
			}
			if (i + 1 != Args.length)
				app.put(',');
		}

		app.put(")values");
		start_ = app.data;
	}

	auto ref duplicateUpdate(string update)
	{
		dupUpdate = cast(char[])update;
		return this;
	}

	void rows(T)(ref const T[] param) if (!isValueType!T)
	{
		if (param.length < 1)
			return;

		foreach (ref p; param)
			row(p);
	}

	private auto tryAppendField(string member, string parentMembers = "", T)(ref const T param, ref size_t fieldHash, ref bool fieldFound)
	{
		static if (isReadableDataMember!(Unqual!T, member))
		{
			alias memberType = typeof(__traits(getMember, param, member));
			static if (isValueType!(memberType))
			{
				enum nameHash = hashOf(parentMembers ~ SQLName!(__traits(getMember, param, member), member));
				if (nameHash == fieldHash)
				{
					appendValue(values_, __traits(getMember, param, member));
					fieldFound = true;
					return;
				}
			}
			else
			{
				foreach (subMember; __traits(allMembers, memberType))
				{
					static if (parentMembers == "")
					{
						tryAppendField!(subMember, member~".")(__traits(getMember, param, member), fieldHash, fieldFound);
					}
					else
					{
						tryAppendField!(subMember, parentMembers~member~".")(__traits(getMember, param, member), fieldHash, fieldFound);
					}

					if (fieldFound)
						return;
				}
			}
		}
	}

	void row(T) (ref const T param) if (!isValueType!T)
	{
		scope (failure) reset();

		if (start_.empty)
			throw new E("Inserter must be initialized with a call to start()");

		if (!pending_)
			values_.put(cast(char[])start_);

		values_.put(pending_ ? ",(" : "(");
		++pending_;

		bool fieldFound;
		foreach (i, ref fieldHash; fieldsHash_)
		{
			fieldFound = false;
			foreach (member; __traits(allMembers, T))
			{
				tryAppendField!member(param, fieldHash, fieldFound);
				if (fieldFound)
					break;
			}
			if (!fieldFound)
				throw new E(format("field '%s' was not found in struct => '%s' members", fieldsNames_.ptr[i], typeid(Unqual!T).name));

			if (i != fields_-1)
				values_.put(',');
		}
		values_.put(')');

		if (values_.data.length > (128 << 10)) // todo: make parameter
			flush();

		++rows_;
	}

	void row(Values...)(Values values) if(allSatisfy!(isValueType, Values))
	{

		scope(failure) reset();

		if (start_.empty)
			throw new E("Inserter must be initialized with a call to start()");

		auto valueCount = values.length;

		foreach (size_t i, Value; Values)
		{
			static if (isArray!Value && !isSomeString!(OriginalType!Value))
			{
				valueCount = (valueCount - 1) + values[i].length;
			}
		}

		if (valueCount != fields_)
			throw new E(format("Wrong number of parameters for row. Got %d but expected %d.", valueCount, fields_));

		if (!pending_)
			values_.put(cast(char[])start_);

		values_.put(pending_ ? ",(" : "(");
		++pending_;
		foreach (size_t i, Value; Values) {
			static if (isArray!Value && !isSomeString!(OriginalType!Value))
				appendValues(values_, values[i]);
			else
				appendValue(values_, values[i]);
			if (i != values.length - 1)
				values_.put(',');
		}
		values_.put(')');

		if (values_.data.length > bufferSize_) // todo: make parameter
			flush();

		++rows_;
	}

	@property size_t rows() const
	{
		return rows_ != 0;
	}

	@property size_t pending() const
	{
		return pending_ != 0;
	}

	@property size_t flushes() const
	{
		return flushes_;
	}

	@property void bufferSize(size_t size)
	{
		bufferSize_ = size;
	}

	@property size_t bufferSize() const
	{
		return bufferSize_;
	}

	private void reset()
	{
		values_.clear;
		pending_ = 0;
	}

	void flush()
	{
		if (pending_)
		{
			if (dupUpdate.length)
			{
				values_.put(cast(ubyte[])" on duplicate key update ");
				values_.put(cast(ubyte[])dupUpdate);
			}

			auto sql = cast(char[])values_.data();
			reset();

			conn_.execute(sql);
			++flushes_;
		}
	}

private:

	char[] start_, dupUpdate;
	Appender!(char[]) values_;

	Connection* conn_;
	size_t pending_;
	size_t flushes_;
	size_t fields_;
	size_t rows_;
	string[] fieldsNames_;
	size_t[] fieldsHash_;
	size_t bufferSize_ = 128 << 10;
}

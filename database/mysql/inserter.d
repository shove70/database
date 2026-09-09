module database.mysql.inserter;

// dfmt off
import
	database.mysql.appender,
	database.mysql.connection,
	database.mysql.exception,
	database.mysql.type,
	database.util,
	std.array,
	std.meta,
	std.range,
	std.string,
	std.traits;
// dfmt on

private {
	alias E = MySQLErrorException;
	enum isSomeStringOrStringArray(T) = isSomeString!(OriginalType!T) ||
		(isArray!T && isSomeString!(ElementType!T));
	enum allStringOrStringArray(T...) = T.length && allSatisfy!(isSomeStringOrStringArray, T);
	enum quote = '`';
}

/++ Action keywords for insert conflict handling. +/
enum OnDuplicate {
	ignore = "insert ignore into ",
	fail = "insert into ",
	replace = "replace into ",
	updateAll = "UpdateAll"
}

/++ Create an inserter with default `insert` behavior. +/
auto inserter(Connection connection) => Inserter(connection);

/++ Create and initialize an inserter for a specific table.

Params:
	action = Conflict handling strategy.
	tableName = Target table.
	columns = Column list.
+/
auto inserter(Args...)(Connection connection, OnDuplicate action, string tableName, Args columns) {
	auto insert = Inserter(connection);
	insert.start(action, tableName, columns);
	return insert;
}

/++ Create and initialize an inserter with default `insert into` action. +/
auto inserter(Args...)(Connection connection, string tableName, Args columns) {
	auto insert = Inserter(connection);
	insert.start(OnDuplicate.fail, tableName, columns);
	return insert;
}

/++ Builds and executes batched MySQL `INSERT` statements.

This helper is optimized for high-throughput writes by buffering many rows and
flushing them as multi-values inserts.
+/
struct Inserter {
	@disable this();
	@disable this(this);

	/++ Create inserter bound to a connection. +/
	this(Connection connection)
	in (connection) {
		conn = connection;
		pending_ = 0;
		flushes_ = 0;
	}

	/++ Flush buffered data on destruction. +/
	~this() {
		flush();
	}

	/++ Start an insert with default `insert into` behavior. +/
	void start(Args...)(string tableName, Args fieldNames)
	if (allStringOrStringArray!Args) {
		start(OnDuplicate.fail, tableName, fieldNames);
	}

	/++ Initialize SQL for buffered insert.

	`action` controls conflict behavior, `tableName` the target table and
	`fieldNames` the target columns.
	+/
	void start(Args...)(OnDuplicate action, string tableName, Args fieldNames)
	if (allStringOrStringArray!Args) {
		auto fieldCount = fieldNames.length;

		foreach (size_t i, Arg; Args) {
			static if (isArray!Arg && !isSomeString!(OriginalType!Arg))
				fieldCount = (fieldCount - 1) + fieldNames[i].length;
		}

		fields_ = fieldCount;

		Appender!(char[]) app;

		if (action == OnDuplicate.updateAll) {
			Appender!(char[]) dupapp;

			foreach (i, Arg; Args) {
				static if (isSomeString!(OriginalType!Arg)) {
					dupapp ~= quote;
					dupapp ~= fieldNames[i];
					dupapp ~= quote;
					dupapp ~= "=values(";
					dupapp ~= quote;
					dupapp ~= fieldNames[i];
					dupapp ~= quote;
					dupapp ~= ')';
				} else {
					auto columns = fieldNames[i];
					foreach (j, name; columns) {
						dupapp ~= quote;
						dupapp ~= name;
						dupapp ~= quote;
						dupapp ~= "=values(";
						dupapp ~= quote;
						dupapp ~= name;
						dupapp ~= quote;
						dupapp ~= ')';
						if (j + 1 != columns.length)
							dupapp ~= ',';
					}
				}
				if (i + 1 != Args.length)
					dupapp ~= ',';
			}
			dupUpdate = dupapp[];
		}
		app ~= cast(char[])action;

		app ~= tableName;
		app ~= '(';

		foreach (i, Arg; Args) {
			static if (isSomeString!(OriginalType!Arg)) {
				fieldsHash ~= hashOf(fieldNames[i]);
				fieldsNames ~= fieldNames[i];

				app ~= quote;
				app ~= fieldNames[i];
				app ~= quote;
			} else {
				auto columns = fieldNames[i];
				foreach (j, name; columns) {
					fieldsHash ~= hashOf(name);
					fieldsNames ~= name;

					app ~= quote;
					app ~= name;
					app ~= quote;
					if (j + 1 != columns.length)
						app ~= ',';
				}
			}
			if (i + 1 != Args.length)
				app ~= ',';
		}

		app ~= ")values";
		start_ = app[];
	}

	/++ Override the `ON DUPLICATE KEY UPDATE` expression and return this inserter. +/
	auto ref duplicateUpdate(string update) {
		dupUpdate = cast(char[])update;
		return this;
	}

	/++ Add multiple aggregate rows at once. +/
	void rows(T)(ref const T[] param) if (!isValueType!T) {
		foreach (ref p; param)
			row(p);
	}

	private bool tryAppendField(string member, string parentMembers = "", T)(
		ref const T param, ref size_t fieldHash) {
		static if (isReadableDataMember!(__traits(getMember, Unqual!T, member))) {
			alias memberType = typeof(__traits(getMember, param, member));
			static if (isValueType!memberType) {
				enum nameHash = hashOf(parentMembers ~ SQLName!(__traits(getMember, param, member), member));
				if (nameHash == fieldHash) {
					appendValue(values_, __traits(getMember, param, member));
					return true;
				}
			} else
				foreach (subMember; __traits(allMembers, memberType)) {
					if (tryAppendField!(subMember, parentMembers ~ member ~ '.')(__traits(getMember, param, member), fieldHash))
						return true;
				}
		}
		return false;
	}

	/++ Add one aggregate row by member reflection. +/
	void row(T)(ref const T param) if (!isValueType!T) {
		scope (failure)
			reset();

		if (start_ == [])
			throw new E("Inserter must be initialized with a call to start()");

		if (!pending_)
			values_ ~= start_;

		values_ ~= pending_ ? ",(" : "(";
		++pending_;

		foreach (i, ref fieldHash; fieldsHash) {
			bool fieldFound;
			foreach (member; __traits(allMembers, T)) {
				fieldFound = tryAppendField!member(param, fieldHash);
				if (fieldFound)
					break;
			}
			if (!fieldFound)
				throw new E("field '%s' was not found in struct => '%s' members".format(fieldsNames.ptr[i], (
						Unqual!T).stringof));

			if (i != fields_ - 1)
				values_ ~= ',';
		}
		values_ ~= ')';

		if (values_[].length > (128 << 10)) // todo: make parameter
			flush();

		++rows_;
	}

	/++ Add one row from positional value list (all values must be value types). +/
	void row(Values...)(Values values) if (allSatisfy!(isValueType, Values)) {
		scope (failure)
			reset();

		if (start_ == [])
			throw new E("Inserter must be initialized with a call to start()");

		auto valueCount = Values.length;

		foreach (i, Value; Values) {
			static if (isArray!Value && !isSomeString!(OriginalType!Value))
				valueCount += values[i].length - 1;
		}

		if (valueCount != fields_)
			throw new E("Wrong number of parameters for row. Got %d but expected %d.".format(valueCount, fields_));

		if (!pending_)
			values_ ~= start_;

		values_ ~= pending_ ? ",(" : "(";
		++pending_;
		foreach (i, Value; Values) {
			static if (isArray!Value && !isSomeString!(OriginalType!Value))
				appendValues(values_, values[i]);
			else
				appendValue(values_, values[i]);
			if (i != values.length - 1)
				values_ ~= ',';
		}
		values_ ~= ')';

		if (values_[].length > bufferSize) // todo: make parameter
			flush();

		++rows_;
	}

	@property {
		/++ Number of rows currently buffered. +/
		size_t rows() const => rows_ != 0;

		/++ Whether any row is pending in the current batch. +/
		size_t pending() const => pending_ != 0;

		/++ Number of successful flush operations. +/
		size_t flushes() const => flushes_;
	}

	/++ Send buffered rows to server and clear the in-memory batch. +/
	void flush() {
		if (pending_) {
			if (dupUpdate.length) {
				values_ ~= " on duplicate key update ";
				values_ ~= dupUpdate;
			}

			auto sql = values_[];
			reset();

			conn.query(cast(string)sql);
			++flushes_;
		}
	}

	size_t bufferSize = 128 << 10;

private:
	void reset() {
		values_.clear;
		pending_ = 0;
	}

	char[] start_, dupUpdate;
	Appender!(char[]) values_;

	Connection conn;
	size_t pending_,
	flushes_,
	fields_,
	rows_;
	string[] fieldsNames;
	size_t[] fieldsHash;
}

@property {
	/++ Build a parenthesized placeholder list for batch inserts.

	If `parens` is false, this returns a raw comma-separated list.
	+/
	string placeholders(size_t x, bool parens = true) {
		import std.array;

		if (!x)
			return null;
		auto app = appender!string;
		if (parens) {
			app.reserve((x << 1) - 1);

			app ~= '(';
			foreach (i; 0 .. x - 1)
				app ~= "?,";
			app ~= '?';
			app ~= ')';
		} else {
			app.reserve(x << 1 | 1);

			foreach (i; 0 .. x - 1)
				app ~= "?,";
			app ~= '?';
		}
		return app[];
	}

	/++ Build a placeholder list from something with a `length` member. +/
	string placeholders(T)(T x, bool parens = true)
	if (is(typeof(() { size_t y = x.length; })))
		=> x.length.placeholders(parens);
}

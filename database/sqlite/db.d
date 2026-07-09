module database.sqlite.db;

// dfmt off
import
	etc.c.sqlite3,
	database.sqlbuilder,
	database.sqlite,
	database.util;
// dfmt on

/++ Setup code for tests. +/
version (unittest) template TEST(string dbName = "") {
	struct User {
		string name;
		int age;
	}

	struct Message {
		@as("rowid") int id;
		string content;
		int byUser;
	}

	mixin database.sqlite.TEST!(dbName, SQLite3DB);
}

/++ Returned from select-type methods where the row type is known.

The wrapper behaves like an input range over typed rows returned by a query.
+/
struct QueryResult(T) {
	Query query;
	alias query this;

	/++ Advance to the next row in the current result set. +/
	void popFront() {
		step();
	}

	/++ The current row mapped to `T`. +/
	@property T front() => this.get!T;
}

unittest {
	QueryResult!int q;
	assert(q.empty);
}

/++ A Database wrapper with query-building and typed CRUD helpers. +/
struct SQLite3DB {
	SQLite3 db;
	alias db this;
	bool autoCreateTable = true;

	/++ Open a SQLite database file and initialize a connection handle. +/
	this(string name, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, int busyTimeout = 500) {
		db = SQLite3(name, flags, busyTimeout);
	}

	/++ Create the backing table for type `T` using SQLBuilder metadata. +/
	bool create(T)() {
		auto q = query(SB.create!T);
		q.step();
		return q.lastCode == SQLITE_DONE;
	}

	/++ Select all rows of `T` matching the SQL expression, returning a typed range. +/
	auto selectAllWhere(T, string expr, A...)(A args) if (expr.length)
		=> QueryResult!T(query(SB.selectAllFrom!T.where(expr), args));

	/++ Select one row of `T` matching the SQL expression and return it.

Throws `SQLEx` when no row exists.
+/
	T selectOneWhere(T, string expr, A...)(A args) if (expr.length) {
		auto q = query(SB.selectAllFrom!T.where(expr), args);
		if (q.step())
			return q.get!T;
		throw new SQLEx("No match");
	}

	/++ Select one row of `T` matching the SQL expression.

Params:
	defValue = Value returned when no row matches.
Returns: A row from the result set, or `defValue` when empty.
+/
	T selectOneWhere(T, string expr, T defValue, A...)(A args)
	if (expr.length) {
		auto q = query(SB.selectAllFrom!T.where(expr), args);
		return q.step() ? q.get!T : defValue;
	}

	/++ Select a row by rowid for the mapped type `T`. +/
	T selectRow(T)(ulong row) => selectOneWhere!(T, "rowid=?")(row);

	unittest {
		mixin TEST;
		import std.array : array;
		import std.algorithm.iteration : fold;

		db.create!User;
		db.insert(User("jonas", 55));
		db.insert(User("oliver", 91));
		db.insert(User("emma", 12));
		db.insert(User("maria", 27));

		auto users = db.selectAllWhere!(User, "age > ?")(20).array;
		auto total = fold!((a, b) => User("", a.age + b.age))(users);

		assert(total.age == 55 + 91 + 27);
		assert(db.selectOneWhere!(User, "age = ?")(27).name == "maria");
		assert(db.selectRow!User(2).age == 91);
	}

	/++ Insert an aggregate `row` using generated SQL and return affected rows. +/
	int insert(OR or = OR.None, alias filter = skipRowid, T)(T row) {
		if (autoCreateTable && !hasTable(SQLName!T)) {
			if (!create!T)
				return 0;
		}
		db.insert!(or, filter)(row).step();
		return db.changes;
	}

	/++ Insert positional arguments into table mapped from `T`.

`fields` may be used to restrict inserted columns.
+/
	int insert(T, string fields = "", OR or = OR.None, A...)(A args) {
		import std.array : split;

		if (autoCreateTable && !hasTable(SQLName!T)) {
			if (!create!T)
				return 0;
		}
		enum f = quoteJoin(fields.split(','));
		enum sql = SB.insert!or(SQLName!T) ~ (f.length ?
					'(' ~ f ~ ")VALUES(" ~ placeholders(f.length) ~ ')' : "");
		return db.exec(sql, args);
	}

	/++ Insert using `OR.Replace` conflict mode for duplicates. +/
	int replaceInto(alias filter = skipRowid, T)(T s) => insert!(OR.Replace, filter, T)(s);

	/++ Delete rows of `T` matching an SQL expression and return affected rows. +/
	int delWhere(T, string expr, A...)(A args) if (expr.length) {
		query(SB.del!T.where(expr), args).step();
		return db.changes;
	}

	unittest {
		mixin TEST;
		User user = {"jonas", 45};
		assert(db.insert(user));
		assert(db.query("select name from User where age = 45").step());
		assert(!db.query("select age from User where name = 'xxx'").step());
		assert(db.delWhere!(User, "age = ?")(45));
	}
}

unittest {
	/// Test quoting by using keyword as table and column name
	mixin TEST;
	struct Group {
		int group;
	}

	Group a = {3};
	db.insert(a);
	Group b = db.selectOneWhere!(Group, `"group"=3`);
	assert(a == b);
}

unittest {
	import std.datetime;

	mixin TEST;

	struct S {
		int id;
		Date date;
		DateTime dt;
		Duration d;
	}

	S a = {
		1, Date(2022, 2, 22), DateTime(2022, 2, 22, 22, 22, 22), dur!"msecs"(666)
	};
	db.insert(a);
	S b = db.selectOneWhere!(S, `id=1`);
	assert(a == b);
}

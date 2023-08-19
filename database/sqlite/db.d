module database.sqlite.db;

// dfmt off
import
	etc.c.sqlite3,
	database.sqlbuilder,
	database.sqlite,
	database.util;
// dfmt on

/// Setup code for tests
version (unittest) template TEST(string dbname = "") {
	struct User {
		string name;
		int age;
	}

	struct Message {
		@as("rowid") int id;
		string content;
		int byUser;
	}

	mixin database.sqlite.TEST!(dbname, SQLite3DB);
}

// Returned from select-type methods where the row type is known
struct QueryResult(T) {
	Query query;
	alias query this;

	@property bool empty() {
		import etc.c.sqlite3;

		if (lastCode < 0)
			step();
		return lastCode != SQLITE_ROW;
	}

	void popFront() {
		step();
	}

	@property T front() => this.get!T;
}

unittest {
	QueryResult!int qi;
	assert(qi.empty);
}

/// A Database with query building capabilities
struct SQLite3DB {
	SQLite3 db;
	alias db this;
	bool autoCreateTable = true;

	this(string name, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, int busyTimeout = 500) {
		db = SQLite3(name, flags, busyTimeout);
	}

	bool create(T)() {
		auto q = query(SB.create!T);
		q.step();
		return q.lastCode == SQLITE_DONE;
	}

	auto selectAllWhere(T, string expr, Args...)(auto ref Args args) if (expr.length)
		=> QueryResult!T(query(SB.selectAllFrom!T.where(expr), args));

	T selectOneWhere(T, string expr, Args...)(auto ref Args args) if (expr.length) {
		auto q = query(SB.selectAllFrom!T.where(expr), args);
		if (q.step())
			return q.get!T;
		throw new SQLEx("No match");
	}

	T selectOneWhere(T, string expr, T defValue, Args...)(auto ref Args args)
	if (expr.length) {
		auto q = query(SB.selectAllFrom!T.where(expr), args);
		return q.step() ? q.get!T : defValue;
	}

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

	int insert(OR or = OR.None, T)(T row) {
		if (autoCreateTable && !hasTable(SQLName!T)) {
			if (!create!T)
				return 0;
		}
		db.insert!or(row).step();
		return db.changes;
	}

	int delWhere(T, string expr, Args...)(auto ref Args args) if (expr.length) {
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
	// Test quoting by using keyword as table and column name
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

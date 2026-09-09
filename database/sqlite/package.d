module database.sqlite;

import std.conv : to;

import database.sqlbuilder,
database.sqlite.query,
database.util,
etc.c.sqlite3,
std.meta,
std.string,
std.traits,
std.typecons;
public import database.sqlite.query;

version (Windows) {
	// manually link in dub.sdl
} else version (linux) {
	pragma(lib, "sqlite3");
} else version (OSX) {
	pragma(lib, "sqlite3");
} else version (Posix) {
	pragma(lib, "libsqlite3");
} else {
	pragma(msg, "You need to manually link in the SQLite library.");
}

/++ Base exception for all SQLite-related errors raised by this module. +/
class SQLiteException : DBException {
	/++ Base exception for SQLite related errors. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure @safe {
		super(msg, file, line);
	}
}

/++ Setup code for tests. +/
version (unittest) package template TEST(string dbName = "", T = SQLite3) {
	T db = {
		static if (dbName.length) {
			tryRemove(dbName ~ ".db");
			return T(dbName ~ ".db");
		} else
			return T(":memory:");
	}();
}

private template Manager(alias ptr, alias freeptr) {
	mixin("alias ", __traits(identifier, ptr), " this;");

	~this() {
		free();
	}

	void free() {
		freeptr(ptr);
		ptr = null;
	}
}

struct ExpandedSql {
	/++ Heap-backed statement text buffer returned by SQLite APIs. +/
	char* ptr;
	mixin Manager!(ptr, sqlite3_free);
}

/++ Alias for a ref-counted expanded SQL wrapper. +/
alias RCExSql = RefCounted!(ExpandedSql, RefCountedAutoInitialize.no);

@property {
	/++ Fetch the last error message for a connection. +/
	auto errmsg(sqlite3* db) => sqlite3_errmsg(db).toStr;

	/++ Return number of rows changed by the last statement. +/
	int changes(sqlite3* db)
	in (db) => sqlite3_changes(db);
	/++ Return the 'rowid' produced by the last insert statement. +/
	long lastRowid(sqlite3* db)
	in (db) => sqlite3_last_insert_rowid(db);

	/++ Set the connection last inserted row id hint. +/
	void lastRowid(sqlite3* db, long rowid)
	in (db) => sqlite3_set_last_insert_rowid(db, rowid);

	/++ Return the cumulative row change count for the connection. +/
	int totalChanges(sqlite3* db)
	in (db) => sqlite3_total_changes(db);

	/++ Return the SQL text associated with a prepared statement. +/
	string sql(sqlite3_stmt* stmt)
	in (stmt) => sqlite3_sql(stmt).toStr;

	/++ Return expanded SQL for a prepared statement with bound values inlined. +/
	RCExSql expandedSql(sqlite3_stmt* stmt)
	in (stmt) => RCExSql(ExpandedSql(sqlite3_expanded_sql(stmt)));
}

/++ A sqlite3 database wrapper and helpers. +/
struct SQLite3 {

	/++ Create a SQLite3 from a database file. If file does not exist, the
		database will be initialized as new
	+/
	this(in char[] dbFile, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, int busyTimeout = 500) {
		const rc = sqlite3_open_v2(dbFile.toz, &db, flags, null);
		if (!rc)
			sqlite3_busy_timeout(db, busyTimeout);
		if (rc != SQLITE_OK) {
			auto errmsg = db.errmsg;
			sqlite3_close(db);
			db = null;
			throw new SQLEx("Could not open database: " ~ errmsg);
		}
	}

	/++ Execute multiple SQL statements separated by semicolons. +/
	int execSQL(in char[] sql, out string errmsg) @trusted {
		char* err_msg = void;
		const rc = sqlite3_exec(db, sql.toz, null, null, &err_msg);
		errmsg = err_msg.toStr;
		return rc;
	}

	/++ Execute an SQL statement directly, binding the args to it. +/
	bool exec(A...)(in char[] sql, A args) {
		auto q = query(sql, args);
		q.step();
		return q.lastCode == SQLITE_DONE || q.lastCode == SQLITE_ROW;
	}

	/// Execute/unittest fixture for statement execution.
	unittest {
		mixin TEST;
		assert(db.exec("CREATE TABLE Test(name STRING)"));
		assert(db.exec("INSERT INTO Test VALUES(?)", "hey"));
	}

	/// Return `true` if database contains the given table.
	bool hasTable(in char[] table) => query(
		"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
		table).step();

	/// Validate table existence checks.
	unittest {
		mixin TEST;
		assert(!db.hasTable("MyTable"));
		db.exec("CREATE TABLE MyTable(id INT)");
		assert(db.hasTable("MyTable"));
	}

	/// Validate insert and rowid behavior.
	unittest {
		mixin TEST;
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES(?)", "hey"));
		assert(db.lastRowid == 1);
		assert(db.exec("INSERT INTO MyTable VALUES(?)", "ho"));
		assert(db.lastRowid == 2);
		/// Only insert updates the last rowid
		assert(db.exec("UPDATE MyTable SET name=? WHERE rowid=?", "woo", 1));
		assert(db.lastRowid == 2);
		db.lastRowid = 9;
		assert(db.lastRowid == 9);
	}

	/++ Create a query from SQL string and bound arguments. +/
	auto query(A...)(in char[] sql, A args)
		=> Query(db, sql, args);

	private auto make(State state, string prefix, string suffix, alias filter = skipRowid, T)(T s)
	if (isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, filter, T);
		// Skips the field
		auto q = Query(db, SB(sql!sqlFields, state));
		foreach (i; FilterIndex!(filter, ColumnNames!T))
			q.set(s.tupleof[i]);
		return q;
	}

	auto insert(OR or = OR.None, alias filter = skipRowid, T)(T s)
	if (isAggregateType!T) {
		import std.array : replicate;

		enum qms = "?,".replicate(ColumnCount!(T, filter));
		return make!(State.insert, or ~ "INTO " ~
				identifier(SQLName!T) ~ '(', ") VALUES(" ~
				(qms.length ? qms[0 .. $ - 1] : qms) ~ ')', filter)(s);
	}

	/++ Begin a transaction. +/
	bool begin() => exec("begin");

	/++ Commit current transaction. +/
	bool commit() => exec("commit");

	/++ Roll back current transaction. +/
	bool rollback() => exec("rollback");

	unittest {
		mixin TEST;
		assert(db.begin());
		assert(db.exec("CREATE TABLE MyTable(name TEXT)"));
		assert(db.exec("INSERT INTO MyTable VALUES(?)", "hey"));
		assert(db.rollback());
		assert(!db.hasTable("MyTable"));
		assert(db.begin());
		assert(db.exec("CREATE TABLE MyTable(name TEXT)"));
		assert(db.exec("INSERT INTO MyTable VALUES(?)", "hey"));
		assert(db.commit());
		assert(db.hasTable("MyTable"));
	}

	auto insertId() => lastRowid(db);

	sqlite3* db;
	alias db this;

	/++ Close the underlying SQLite handle. +/
	void close() {
		sqlite3_close_v2(db);
		db = null;
	}
}

shared static this() {
	const c = sqlite3_initialize();
	assert(c == SQLITE_OK, "Failed to initialize SQLite: " ~ c.to!string);
}

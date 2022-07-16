module database.sqlite;

import std.conv : to;
import
	std.exception,
	std.meta,
	std.string,
	std.traits,
	std.typecons,
	etc.c.sqlite3,
	database.sqlbuilder,
	database.util;

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

class SQLiteException : DBException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/// Setup code for tests
version(unittest) package template TEST(string dbname = "", T = SQLite3) {
	T db = () {
		static if (dbname.length) {
			tryRemove(dbname ~ ".db");
			return new T(dbname ~ ".db");
		} else
			return new T(":memory:");
	}();
}

package {
	alias SQLEx = SQLiteException;

	void checkError(sqlite3* db, string prefix, int rc,
		string file = __FILE__, int line = __LINE__)
	in(db) {
		if(rc < 0)
			rc = sqlite3_errcode(db);
		enforce!SQLEx(
			rc == SQLITE_OK || rc == SQLITE_ROW || rc == SQLITE_DONE, prefix ~
			" (" ~ rc.to!string ~ "): " ~ db.errmsg, file, line);
	}
}

template Manager(alias ptr, alias freeptr) {
	mixin("alias ", __traits(identifier, ptr), " this;");
	~this() { free(); }
	void free() { freeptr(ptr); ptr = null; }
}

struct ExpandedSql {
	char* ptr;
	mixin Manager!(ptr, sqlite3_free);
}

alias RCExSql = RefCounted!(ExpandedSql, RefCountedAutoInitialize.no);

@property {
	auto errmsg(sqlite3* db) {
		return sqlite3_errmsg(db).toStr;
	}

	int changes(sqlite3* db) in(db) {
		return sqlite3_changes(db);
	}
	/// Return the 'rowid' produced by the last insert statement
	long lastRowid(sqlite3* db) in(db) {
		return sqlite3_last_insert_rowid(db);
	}

	void lastRowid(sqlite3* db, long rowid) in(db) {
		sqlite3_set_last_insert_rowid(db, rowid);
	}

	int totalChanges(sqlite3* db) in(db) {
		return sqlite3_total_changes(db);
	}

	string sql(sqlite3_stmt* stmt) in(stmt) {
		return sqlite3_sql(stmt).toStr;
	}

	RCExSql expandedSql(sqlite3_stmt* stmt) in(stmt) {
		return RCExSql(ExpandedSql(sqlite3_expanded_sql(stmt)));
	}
}

/// Represents a sqlite3 statement
struct Statement
{
	int lastCode;
	int argIndex;
	sqlite3_stmt* stmt;
	mixin Manager!(stmt, sqlite3_finalize);

	/// Construct a query from the string 'sql' into database 'db'
	this(ARGS...)(sqlite3* db, string sql, ARGS args)
	in(db)
	in(sql.length) {
		lastCode = -1;
		int rc = sqlite3_prepare_v2(db, sql.toz, -1, &stmt, null);
		db.checkError("Prepare failed: ", rc);
		this.db = db;
		set(args);
	}

	alias close = free;

private:
	sqlite3* db;

	int bindArg(S)(int pos, S arg) if(isSomeString!S) {
		static if(size_t.sizeof > 4)
			return sqlite3_bind_text64(stmt, pos, arg.ptr, arg.length, null, SQLITE_UTF8);
		else
			return sqlite3_bind_text(stmt, pos, arg.ptr, cast(int)arg.length, null);
	}

	int bindArg(int pos, double arg) {
		return sqlite3_bind_double(stmt, pos, arg);
	}

	int bindArg(T)(int pos, T arg) if(isIntegral!T) {
		static if(T.sizeof > 4)
			return sqlite3_bind_int64(stmt, pos, arg);
		else
			return sqlite3_bind_int(stmt, pos, arg);
	}

	int bindArg(int pos, void[] arg) {
		static if(size_t.sizeof > 4)
			return sqlite3_bind_blob64(stmt, pos, arg.ptr, arg.length, null);
		else
			return sqlite3_bind_blob(stmt, pos, arg.ptr, cast(int)arg.length, null);
	}

	int bindArg(T)(int pos, T) if (is(Unqual!T : typeof(null))) {
		return sqlite3_bind_null(stmt, pos);
	}

	T getArg(T)(int pos) in(stmt) {
		int typ = sqlite3_column_type(stmt, pos);
		static if(isIntegral!T) {
			enforce!SQLEx(typ == SQLITE_INTEGER, "Column is not an integer");
			static if(T.sizeof > 4)
				return sqlite3_column_int64(stmt, pos);
			else
				return cast(T)sqlite3_column_int(stmt, pos);
		} else static if(isSomeString!T) {
			if (typ == SQLITE_NULL)
				return T.init;
			int size = sqlite3_column_bytes(stmt, pos);
			return cast(T)sqlite3_column_text(stmt, pos)[0..size].dup;
		} else static if(isFloatingPoint!T) {
			enforce!SQLEx(typ != SQLITE_BLOB, "Column cannot convert to a real");
			return sqlite3_column_double(stmt, pos);
		} else {
			if (typ == SQLITE_NULL)
				return T.init;
			enforce!SQLEx(typ == SQLITE3_TEXT || typ == SQLITE_BLOB,
				"Column is not a blob or string");
			auto ptr = sqlite3_column_blob(stmt, pos);
			int size = sqlite3_column_bytes(stmt, pos);
			static if(isStaticArray!T)
				return cast(T)ptr[0..size];
			else
				return cast(T)ptr[0..size].dup;
		}
	}

public:
	/// Bind these args in order to '?' marks in statement
	void set(ARGS...)(ARGS args) {
		static foreach(a; args)
			db.checkError("Bind failed: ", bindArg(++argIndex, a));
	}

	int clear() in(stmt) { return sqlite3_clear_bindings(stmt); }

	// Find column by name
	int findColumn(string name) in(stmt) {
		import core.stdc.string : strcmp;

		auto ptr = name.toz;
		int count = sqlite3_column_count(stmt);
		for(int i = 0; i < count; i++) {
			if(strcmp(sqlite3_column_name(stmt, i), ptr) == 0)
				return i;
		}
		return -1;
	}

	/// Get current row (and column) as a basic type
	T get(T, int COL = 0)() if(!isAggregateType!T)
	{
		if(lastCode == -1)
			step();
		return getArg!T(COL);
	}

	/// Map current row to the fields of the given T
	T get(T, int _ = 0)() if(isAggregateType!T)
	{
		if(lastCode == -1)
			step();
		T t;
		int i = void;
		static foreach(N; FieldNameTuple!T) {
			i = findColumn(ColumnName!(T, N));
			if(i >= 0)
				__traits(getMember, t, N) = getArg!(typeof(__traits(getMember, t, N)))(i);
		}
		return t;
	}

	/// Get current row as a tuple
	Tuple!T get(T...)()
	{
		Tuple!T t;
		foreach(I, Ti; T)
			t[I] = get!(Ti, I)();
		return t;
	}

	/// Step the SQL statement; move to next row of the result set. Return `false` if there are no more rows
	bool step()
	in(stmt) {
		lastCode = sqlite3_step(stmt);
		db.checkError("Step failed", lastCode);
		return lastCode == SQLITE_ROW;
	}

	/// Reset the statement, to step through the resulting rows again.
	void reset() in(stmt) { sqlite3_reset(stmt); }
}

///
unittest {
	mixin TEST;

	auto q = db.query("create table TEST(a INT, b INT)");
	assert(!q.step());

	q = db.query("insert into TEST values(?, ?)");
	q.set(1, 2);
	assert(!q.step());
	q = db.query("select b from TEST where a == ?", 1);
	assert(q.step());
	assert(q.get!int == 2);
	assert(!q.step());

	q = db.query("select a,b from TEST where b == ?", 2);
	// Try not stepping... assert(q.step());
	assert(q.get!(int, int) == tuple(1, 2));

	struct Test { int a, b; }

	auto test = q.get!Test;
	assert(test.a == 1 && test.b == 2);

	assert(!q.step());

	q.reset();
	assert(q.step());
	assert(q.get!(int, int) == tuple(1, 2));

	// Test exception
	assertThrown!SQLEx(q.get!(byte[]));
}

alias Query = RefCounted!Statement;

/// A sqlite3 database
class SQLite3
{
	protected alias SB = SQLBuilder;

	/** Create a SQLite3 from a database file. If file does not exist, the
	  * database will be initialized as new
	 */
	this(string dbFile, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, int busyTimeout = 500)
	{
		int rc = sqlite3_open_v2(dbFile.toz, &db, flags, null);
		if (!rc)
			sqlite3_busy_timeout(db, busyTimeout);
		if(rc != SQLITE_OK) {
			auto errmsg = db.errmsg;
			sqlite3_close(db);
			db = null;
			throw new SQLEx("Could not open database: " ~ errmsg);
		}
	}

	/// Execute multiple statements
	int execSQL(string sql, out string errmsg)
	{
		char* err_msg = void;
		int rc = sqlite3_exec(db, sql.toz, null, null, &err_msg);
		errmsg = err_msg.toStr;
		return rc;
	}

	/// Execute an sql statement directly, binding the args to it
	bool exec(ARGS...)(string sql, ARGS args)
	{
		auto q = query(sql, args);
		q.step();
		return q.lastCode == SQLITE_DONE || q.lastCode == SQLITE_ROW;
	}

	///
	unittest {
		mixin TEST;
		assert(db.exec("CREATE TABLE Test(name STRING)"));
		assert(db.exec("INSERT INTO Test VALUES (?)", "hey"));
	}

	/// Return 'true' if database contains the given table
	bool hasTable(string table)
	{
		return query("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			table).step();
	}

	///
	unittest {
		mixin TEST;
		assert(!db.hasTable("MyTable"));
		db.exec("CREATE TABLE MyTable(id INT)");
		assert(db.hasTable("MyTable"));
	}

	///
	unittest {
		mixin TEST;
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		assert(db.lastRowid == 1);
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "ho"));
		assert(db.lastRowid == 2);
		// Only insert updates the last rowid
		assert(db.exec("UPDATE MyTable SET name=? WHERE rowid=?", "woo", 1));
		assert(db.lastRowid == 2);
		db.lastRowid = 9;
		assert(db.lastRowid == 9);
	}

	/// Create query from string and args to bind
	auto query(ARGS...)(string sql, ARGS args)
	{
		return Query(db, sql, args);
	}

	private auto make(State state, string prefix, string suffix, T)(T s)
	if(isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, T);
		// Skips "rowid" field
		static if(I >= 0)
			return Statement(db, SB(sql!sqlFields, state),
				s.tupleof[0..I], s.tupleof[I+1..$]);
		else
			return Statement(db, SB(sql!sqlFields, state), s.tupleof);
	}

	auto insert(OR or = OR.None, T)(T s)
	if(isAggregateType!T) {
		import std.array : replicate;

		enum qms = ",?".replicate(ColumnCount!T);
		return make!(State.insert, or ~ "INTO " ~
			quote(SQLName!T) ~ '(', ") VALUES(" ~
				(qms.length ? qms[1..$] : qms) ~ ')')(s);
	}

	bool commit() { return exec("commit"); }
	bool begin() { return exec("begin"); }
	bool rollback() { return exec("rollback"); }

	unittest {
		mixin TEST;
		assert(db.begin());
		assert(db.exec("CREATE TABLE MyTable(name TEXT)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		assert(db.rollback());
		assert(!db.hasTable("MyTable"));
		assert(db.begin());
		assert(db.exec("CREATE TABLE MyTable(name TEXT)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		assert(db.commit());
		assert(db.hasTable("MyTable"));
	}

	protected sqlite3 *db;
	mixin Manager!(db, sqlite3_close_v2);
	alias close = free;
}
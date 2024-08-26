module database.sqlite.query;

import database.sqlite,
database.traits,
etc.c.sqlite3,
std.datetime,
std.exception,
std.string,
std.traits,
std.typecons;

enum EpochDateTime = DateTime(2000, 1, 1, 0, 0, 0);
enum EpochStdTime = 630_822_816_000_000_000; //SysTime(EpochDateTime, UTC()).stdTime

private enum canConvertToInt(T) = __traits(isIntegral, T) ||
	is(T : Date) || is(T : DateTime) || is(T : SysTime) || is(T : Duration);

/// Represents a sqlite3 statement
alias Statement = Query;

struct Query {
	int lastCode;
	int argIndex;
	sqlite3_stmt* stmt;
	alias stmt this;

	/// Construct a query from the string 'sql' into database 'db'
	this(A...)(sqlite3* db, in char[] sql, auto ref A args)
	in (db)
	in (sql.length <= int.max) {
		lastCode = -1;
		_rc = 1;
		const rc = sqlite3_prepare_v2(db, sql.ptr, cast(int)sql.length, &stmt, null);
		db.checkError!"Prepare failed: "(rc);
		this.db = db;
		static if (A.length)
			set(args);
	}

	this(this) {
		_rc++;
	}

	~this() {
		if (--_rc == 0)
			close();
	}

	/// Close the statement
	void close() {
		sqlite3_finalize(stmt);
		stmt = null;
	}

	/// Bind these args in order to '?' marks in statement
	pragma(inline, true) void set(A...)(auto ref A args) {
		foreach (a; args)
			db.checkError!"Bind failed: "(bindArg(++argIndex, a));
	}

	int clear()
	in (stmt) => sqlite3_clear_bindings(stmt);

	/// Find column by name
	int findColumn(in char[] name)
	in (stmt) {
		import core.stdc.string : strcmp;

		auto ptr = name.toz;
		const count = sqlite3_column_count(stmt);
		for (int i = 0; i < count; i++) {
			if (strcmp(sqlite3_column_name(stmt, i), ptr) == 0)
				return i;
		}
		return -1;
	}

	/// ditto
	int findColumn(scope const char* namez)
	in (stmt) {
		import core.stdc.string : strcmp;

		const count = sqlite3_column_count(stmt);
		for (int i = 0; i < count; i++) {
			if (strcmp(sqlite3_column_name(stmt, i), namez) == 0)
				return i;
		}
		return -1;
	}

	auto ref front() => this;

	alias popFront = step;

	/// Get current row (and column) as a basic type
	T get(T, int column = 0)() if (!isAggregateType!T)
	in (stmt) {
		if (lastCode == -1)
			step();
		return getArg!T(column);
	}

	/// Map current row to the fields of the given T
	T get(T, int _ = 0)() if (isAggregateType!T)
	in (stmt) {
		if (lastCode == -1)
			step();
		T t;
		int i = void;
		foreach (N; FieldNameTuple!T) {
			i = findColumn(ColumnName!(T, N).ptr);
			if (i >= 0)
				__traits(getMember, t, N) = getArg!(typeof(__traits(getMember, t, N)))(i);
		}
		return t;
	}

	/// Get current row as a tuple
	Tuple!T get(T...)() {
		Tuple!T t;
		foreach (I, Ti; T)
			t[I] = get!(Ti, I)();
		return t;
	}

	/// Step the SQL statement; move to next row of the result set. Return `false` if there are no more rows
	bool step()
	in (stmt) {
		db.checkError!"Step failed"(lastCode = sqlite3_step(stmt));
		return lastCode == SQLITE_ROW;
	}

	@property bool empty() {
		if (lastCode == -1)
			step();
		return lastCode != SQLITE_ROW;
	}

	T opCast(T : bool)() => !empty; // @suppress(dscanner.suspicious.object_const)

	/// Reset the statement, to step through the resulting rows again.
	int reset()
	in (stmt) => sqlite3_reset(stmt);

private:
	sqlite3* db;
	size_t _rc;

	int bindArg(int pos, const char[] x) {
		static if (size_t.sizeof > 4)
			return sqlite3_bind_text64(stmt, pos, x.ptr, x.length, null, SQLITE_UTF8);
		else
			return sqlite3_bind_text(stmt, pos, x.ptr, cast(int)x.length, null);
	}

	int bindArg(int pos, double x)
		=> sqlite3_bind_double(stmt, pos, x);

	int bindArg(T)(int pos, T x) if (canConvertToInt!T) {
		static if (is(T : Date))
			return sqlite3_bind_int(stmt, pos, x.dayOfGregorianCal);
		else static if (is(T : DateTime))
			return sqlite3_bind_int64(stmt, pos, (x - EpochDateTime).total!"usecs");
		else static if (is(T : SysTime))
			return sqlite3_bind_int64(stmt, pos, x.stdTime - EpochStdTime);
		else static if (is(T : Duration))
			return sqlite3_bind_int64(stmt, pos, x.total!"usecs");
		else static if (T.sizeof > 4)
			return sqlite3_bind_int64(stmt, pos, x);
		else
			return sqlite3_bind_int(stmt, pos, x);
	}

	int bindArg(int pos, const void[] x) {
		static if (size_t.sizeof > 4)
			return sqlite3_bind_blob64(stmt, pos, x.ptr, x.length, null);
		else
			return sqlite3_bind_blob(stmt, pos, x.ptr, cast(int)x.length, null);
	}

	int bindArg(int pos, typeof(null))
		=> sqlite3_bind_null(stmt, pos);

	T getArg(T)(int pos) {
		const typ = sqlite3_column_type(stmt, pos);
		static if (canConvertToInt!T) {
			enforce!SQLEx(typ == SQLITE_INTEGER, "Column is not an integer");
			static if (is(T : Date))
				return Date(sqlite3_column_int(stmt, pos));
			else static if (is(T : DateTime))
				return EpochDateTime + dur!"usecs"(sqlite3_column_int64(stmt, pos));
			else static if (is(T : SysTime))
				return SysTime(sqlite3_column_int64(stmt, pos) + EpochStdTime);
			else static if (is(T : Duration))
				return dur!"usecs"(sqlite3_column_int64(stmt, pos));
			else static if (T.sizeof > 4)
				return sqlite3_column_int64(stmt, pos);
			else
				return cast(T)sqlite3_column_int(stmt, pos);
		} else static if (isSomeString!T) {
			if (typ == SQLITE_NULL)
				return T.init;
			int size = sqlite3_column_bytes(stmt, pos);
			return cast(T)sqlite3_column_text(stmt, pos)[0 .. size].dup;
		} else static if (isFloatingPoint!T) {
			enforce!SQLEx(typ != SQLITE_BLOB, "Column cannot convert to a real");
			return sqlite3_column_double(stmt, pos);
		} else {
			if (typ == SQLITE_NULL)
				return T.init;
			enforce!SQLEx(typ == SQLITE3_TEXT || typ == SQLITE_BLOB,
				"Column is not a blob or string");
			auto ptr = sqlite3_column_blob(stmt, pos);
			int size = sqlite3_column_bytes(stmt, pos);
			static if (isStaticArray!T) {
				enforce!SQLEx(size == T.sizeof, "Column size does not match array size");
				return cast(T)ptr[0 .. T.sizeof];
			} else
				return cast(T)ptr[0 .. size].dup;
		}
	}
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

	struct Test {
		int a, b;
	}

	auto test = q.get!Test;
	assert(test.a == 1 && test.b == 2);

	assert(!q.step());

	q.reset();
	assert(q.step());
	assert(q.get!(int, int) == tuple(1, 2));

	// Test exception
	assertThrown!SQLEx(q.get!(byte[]));
}

package:

alias SQLEx = SQLiteException;
alias toz = toStringz;

void checkError(string prefix)(sqlite3* db, int rc) {
	import std.conv : to;

	if (rc < 0)
		rc = sqlite3_errcode(db);
	enforce!SQLEx(rc == SQLITE_OK || rc == SQLITE_ROW || rc == SQLITE_DONE,
		prefix ~ " (" ~ rc.to!string ~ "): " ~ db.errmsg);
}

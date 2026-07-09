module database.sqlbuilder;
// dfmt off
import
	database.util,
	std.ascii,
	std.meta,
	std.range,
	std.traits;
// dfmt on
import std.string;
public import database.traits : SQLName;

/++ SQL clause fragments used to assemble statements. +/
enum State {
	none = "",
	create = "CREATE TABLE ",
	createNX = "CREATE TABLE IF NOT EXISTS ",
	del = "DELETE FROM ",
	from = " FROM ",
	groupBy = " GROUP BY ",
	having = " HAVING ",
	insert = "INSERT ",
	limit = " LIMIT ",
	offset = " OFFSET ",
	orderBy = " ORDER BY ",
	returning = " RETURNING ",
	select = "SELECT ",
	set = " SET ",
	update = "UPDATE ",
	where = " WHERE "
}

/++ Optional `OR` behaviors used in insert statements. +/
enum OR {
	None = "",
	Abort = "OR ABORT ",
	Fail = "OR FAIL ",
	Ignore = "OR IGNORE ",
	Replace = "OR REPLACE ",
	Rollback = "OR ROLLBACK "
}

@safe:

/++ Build a comma-separated list of numbered SQL placeholders (`$1`, `$2` ...).

Params:
	x = Number of placeholders to generate.
Returns: A placeholder list or an empty string.
+/
string placeholders(size_t x) pure nothrow {
	import std.array;

	if (!x)
		return "";

	auto s = appender!string;
	placeholders(x, s);
	return s[];
}

/++ Append numbered SQL placeholders into an appender.

Params:
	x = Number of placeholders.
	s = Destination appender-like object.
+/
void placeholders(R)(size_t x, ref scope R s) {
	import std.conv : to;

	if (!x)
		return;

	s.put("$1");
	foreach (i; 2 .. x + 1) {
		s.put(",$");
		s.put(i.to!string);
	}
}

/++ An instance of a query building process. +/
struct SQLBuilder {
	string sql;
	alias sql this;
	State state;

	/++ Construct a builder with an initial SQL fragment and state.

	Params:
		sql = SQL fragment content.
		STATE = Initial clause state.
	+/
	this(string sql, State STATE = State.none) {
		this.sql = STATE.startsWithWhite ? sql : STATE ~ sql;
		state = STATE;
	}

	/++ Generate a CREATE TABLE statement for an aggregate type.

	Params:
		T = Aggregate type defining table schema.
	Returns: A SQL builder carrying a CREATE TABLE statement.
	+/
	static SB create(T)() if (isAggregateType!T) {
		enum sql = createTable!T;
		return sql;
	}

	/// `create` examples and compile-time checks.
	unittest {
		assert(SQLBuilder.create!User == `CREATE TABLE IF NOT EXISTS "User"(name TEXT,age INT)`);
		static assert(!__traits(compiles, SQLBuilder.create!int));
	}

	/++ Build an INSERT statement.

	Overloads:
		- `insert(OR or, const(char)[] table)`
		- `insert(OR or, T)()`
		- `insert(OR or, alias filter, T)()` (filtered columns)
	Returns: A SQL builder for insert operations.
	+/
	static SB insert(OR or = OR.None, S:
		const(char)[])(S table)
		=> SB(or ~ "INTO " ~ identifier(table), State.insert);

	static SB insert(OR or = OR.None, T)()
		=> SB(or ~ "INTO " ~ identifier(SQLName!T), State.insert);

	alias insert(T, alias filter = skipRowid) = insert!(OR.None, filter, T);

	static SB insert(OR or = OR.None, alias filter = skipRowid, T)()
	if (isAggregateType!T && __traits(isTemplate, filter)) {
		mixin make!(or ~ "INTO " ~ identifier(SQLName!T) ~ '(', ")VALUES(", filter, T);
		return SB(make ~ placeholders(sqlFields.length) ~ ')', State.insert);
	}

	/// `insert` overload coverage checks.
	unittest {
		assert(SQLBuilder.insert("User") == `INSERT INTO "User"`);
		assert(SQLBuilder.insert!(OR.Ignore, User) == `INSERT OR IGNORE INTO "User"`, SQLBuilder.insert!(OR.Ignore, skipRowid, User));
		assert(SQLBuilder.insert!User == `INSERT INTO "User"(name,age)VALUES($1,$2)`);
		assert(SQLBuilder.insert!Message == `INSERT INTO msg(contents)VALUES($1)`);
	}

	/++ Build a SELECT statement from field names or aggregate types. +/
	static SB select(Args...)() if (Args.length) {
		static if (allSatisfy!(isString, Args)) {
			enum sql = [Args].join(',');
			return SB(sql, State.select);
		} else {
			enum sql = quoteJoin([staticMap!(SQLName, Args)]);
			enum isTable(alias x) = is(x) && isAggregateType!x;
			static if (allSatisfy!(isTable, Args)) {
				return SB("*", State.select).from(sql);
			} else {
				return SB(sql, State.select).from(NoDuplicates!(staticMap!(ParentName, Args)));
			}
		}
	}

	/// `select` coverage checks with field/table overload inputs.
	unittest {
		assert(SQLBuilder.select!("only_one") == `SELECT only_one`);
		assert(SQLBuilder.select!("hey", "you") == `SELECT hey,you`);
		assert(SQLBuilder.select!(User.name) == `SELECT name FROM "User"`);
		assert(SQLBuilder.select!(User.name, User.age) == `SELECT name,age FROM "User"`);
		assert(SQLBuilder.select!User == `SELECT * FROM "User"`);
		with (User) {
			assert(SQLBuilder.select!(name, age) == `SELECT name,age FROM "User"`);
		}
	}

	/++ Build a SELECT * statement for one or more aggregate table types. +/
	static SB selectAllFrom(Tables...)() if (allSatisfy!(isAggregateType, Tables)) {
		string[] fields, tables;
		foreach (S; Tables) {
			{
				enum tblName = SQLName!S;
				foreach (N; FieldNameTuple!S)
					fields ~= identifier(tblName) ~ '.' ~ identifier(ColumnName!(S, N));

				tables ~= tblName;
			}
		}
		return SB("SELECT " ~ fields.join(',') ~ " FROM "
				~ quoteJoin(tables), State.from);
	}
	/// `selectAllFrom` compile check with multiple table inputs.
	unittest {
		assert(SQLBuilder.selectAllFrom!(Message, User) ==
				`SELECT msg.rowid,msg.contents,"User".name,"User".age FROM msg,"User"`);
	}

	/++ Add generated clause helpers for FROM/SET/SELECT. +/
	mixin(Clause!("from", "set", "select"));

	/++ Add one or more FROM table names (raw values). +/
	SB from(Tables...)(Tables tables)
		if (Tables.length > 1 && allSatisfy!(isString, Tables))
		=> from([tables].join(','));

	/++ Add FROM clause from one or more aggregate tables. +/
	SB from(Tables...)() if (Tables.length && allSatisfy!(isAggregateType, Tables))
		=> from(quoteJoin([staticMap!(SQLName, Tables)]));

	/++ Add a subquery as FROM source. +/
	SB from()(SB subquery) {
		sql ~= (state = State.from) ~ '(' ~ subquery.sql ~ ')';
		return this;
	}

	/++ Add generated clause helpers for SET/UPDATE. +/
	mixin(Clause!("set", "update"));

	/++ Build an UPDATE statement.

Overloads:
	- `update(OR or, const(char)[])`
	- `update(T, OR or)()`
	- `updateAll(T, OR or, filter)()`
	+/
	static SB update(OR or = OR.None, S:
		const(char)[])(S table)
		=> SB(or ~ identifier(table), State.update);

	static SB update(T, OR or = OR.None)() if (isAggregateType!T)
		=> SB(or ~ identifier(SQLName!T), State.update);

	static SB updateAll(T, OR or = OR.None, alias filter = skipRowid)()
	if (isAggregateType!T)
		=> SB(make!("UPDATE " ~ or ~ identifier(SQLName!T) ~ " SET ", "=?", filter, T), State.set);

	/// `update` overload coverage checks.
	unittest {
		assert(SQLBuilder.update("User") == `UPDATE "User"`);
		assert(SQLBuilder.update!User == `UPDATE "User"`);
		assert(SQLBuilder.update!User.set("name=$1") == `UPDATE "User" SET name=$1`);
		assert(SQLBuilder.updateAll!User == `UPDATE "User" SET name=$1,age=$2`);
	}

	/++ Add generated clause helpers for WHERE/SET/FROM/DELETE. +/
	mixin(Clause!("where", "set", "from", "del"));

	/++ Build a DELETE statement.

Overloads:
	- `del(Table)()`
	- `del(string table)`
	+/
	static SB del(Table)() if (isAggregateType!Table)
		=> del(identifier(SQLName!Table));

	static SB del(string table)
		=> SB(table, State.del);

	/// `del` overload coverage checks.
	unittest {
		assert(SQLBuilder.del!User.where("name=$1") ==
				`DELETE FROM "User" WHERE name=$1`);
		assert(SQLBuilder.del!User.returning("*") ==
				`DELETE FROM "User" RETURNING *`);
	}

	/++ Add generated clause helpers for USING/DELETE. +/
	mixin(Clause!("using", "del"));

	/++ Add generated clause helpers for GROUP BY. +/
	mixin(Clause!("groupBy", "from", "where"));

	/++ Add generated clause helpers for HAVING. +/
	mixin(Clause!("having", "from", "where", "groupBy"));

	/++ Add generated clause helpers for ORDER BY. +/
	mixin(Clause!("orderBy", "from", "where", "groupBy", "having"));

	/++ Add generated clause helpers for LIMIT. +/
	mixin(Clause!("limit", "from", "where", "groupBy", "having", "orderBy"));

	/++ Add generated clause helpers for OFFSET. +/
	mixin(Clause!("offset", "limit"));

	/++ Add generated clause helpers for RETURNING. +/
	mixin(Clause!("returning"));

	/++ Append a raw SQL expression to the current SQL builder.

	Params:
		expr = SQL expression.
	Returns: The updated statement builder.
	+/
	SB opCall(const(char)[] expr) {
		sql ~= expr;
		return this;
	}

private:
	enum Clause(string name, prevStates...) =
		"SB " ~ name ~ "(const(char)[] expr)" ~
		(prevStates.length ? "in(state == State." ~ [prevStates].join!(
				string[])(
				" || state == State.") ~ `, "Wrong SQL: ` ~ name ~ ` after " ~ state)` : "")
		~ "{ sql ~= " ~ (__traits(hasMember, State, name) ?
				"(state = State." ~ name ~ ")" : `" ` ~ name.toUpper ~ ` "`) ~ " ~ expr;
		return this;}";

	template make(string prefix, string suffix, alias filter, T)
	if (isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, filter, T);
		enum make = sql!sqlFields;
	}
}

/// Test helpers for `SQLBuilder` examples.
unittest {
		/// This will map to a "User" table in our database
		struct User {
		string name;
		int age;
	}

	assert(SB.create!User == `CREATE TABLE IF NOT EXISTS "User"(name TEXT,age INT)`);

	auto q = SB.select!"name"
		.from!User
		.where("age=$1");

	/// The properties `sql` can be used to access the generated sql
	assert(q.sql == `SELECT name FROM "User" WHERE age=$1`);

	/// We can decorate structs and fields to give them different names in the database.
	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}

	/// Note that virtual "rowid" field is handled differently -- it will not be created
	/// by create(), and not inserted into by insert()

	assert(SB.create!Message == `CREATE TABLE IF NOT EXISTS msg(contents TEXT)`);

	auto q2 = SB.insert!Message;
	assert(q2 == `INSERT INTO msg(contents)VALUES($1)`);
}

unittest {
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;

	alias C = ColumnName;

	/// Make sure all these generate the same sql statement
	auto sql = [
		SB.select!(`msg.rowid`, `msg.contents`).from(`msg`)
			.where(`msg.rowid=$1`).sql,
		SB.select!(`msg.rowid`, `msg.contents`)
			.from!Message
			.where(C!(Message.id) ~ "=$1").sql,
		SB.select!(C!(Message.id), C!(Message.contents))
			.from!Message
			.where(`msg.rowid=$1`).sql,
		SB.selectAllFrom!Message.where(`msg.rowid=$1`).sql
	];
	assert(count(uniq(sql)) == 1);
}

private:

enum isString(alias x) = __traits(compiles, { const(char)[] s = x; });

bool startsWithWhite(S)(S s)
	=> s.length && s[0].isWhite;

SB createTable(T)() {
	string s;
	static foreach (A; __traits(getAttributes, T))
		static if (is(typeof(A) : const(char)[]))
			static if (A.length) {
				static if (A.startsWithWhite)
					s ~= A;
				else
					s ~= ' ' ~ A;
			}
	alias FIELDS = Fields!T;
	string[] fields, keys, pkeys;

	static foreach (I, colName; ColumnNames!T)
		static if (colName.length) {
			{
				static if (colName != "rowid") {
					string field = identifier(colName) ~ ' ',
					type = SQLTypeOf!(FIELDS[I]),
					constraints;
				}
				static foreach (A; __traits(getAttributes, T.tupleof[I]))
					static if (is(typeof(A) == sqlkey)) {
						static if (A.key.length) {
							{
								enum key = "FOREIGN KEY(" ~ identifier(
										colName) ~ ") REFERENCES " ~ A.key;
								version (DB_SQLite)
									keys ~= key ~ " ON DELETE CASCADE";
								else
									keys ~= key;
							}
						} else
							pkeys ~= colName;
					} else static if (colName != "rowid" && is(typeof(A) == sqltype))
						type = A.type;
					else static if (is(typeof(A) : const(char)[]))
						static if (A.length) {
							static if (A.startsWithWhite)
								constraints ~= A;
							else
								constraints ~= ' ' ~ A;
						}
				static if (colName != "rowid") {
					field ~= type ~ constraints;
					enum member = T.init.tupleof[I];
					if (member != FIELDS[I].init)
						field ~= " default " ~ toSQLValue(member);
					fields ~= field;
				}
			}
		}
	if (pkeys.length)
		keys ~= "PRIMARY KEY(" ~ quoteJoin(pkeys) ~ ')';

	return SB(identifier(SQLName!T) ~ '(' ~ join(fields ~ keys, ',') ~ ')'
			~ s, State.createNX);
}

string toSQLValue(T)(T value) {
	import std.datetime,
	std.conv : to;

	auto x = cast(OriginalType!(Unqual!T))value;
	static if (__traits(isIntegral, T))
		return to!string(cast(long)x);
	else static if (is(T : Date))
		return to!string(x.dayOfGregorianCal);
	else static if (is(T : Duration))
		return to!string(x.total!"usecs");
	else {
		version (DB_SQLite) {
			import database.sqlite;

			static if (is(T : DateTime))
				return to!string((x - EpochDateTime).total!"usecs");
			else static if (is(T : SysTime))
				return to!string(x.stdTime - EpochStdTime);
			else
				return quote(x.to!string);
		} else
			return quote(x.to!string);
	}
}

package(database) alias SB = SQLBuilder;

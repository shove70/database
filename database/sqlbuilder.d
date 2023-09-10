module database.sqlbuilder;
// dfmt off
import
	database.util,
	std.ascii,
	std.meta,
	std.range,
	std.traits;
// dfmt on
import std.string : join, count;
public import database.traits : SQLName;

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
	select = "SELECT ",
	set = " SET ",
	update = "UPDATE ",
	where = " WHERE "
}

enum OR {
	None = "",
	Abort = "OR ABORT ",
	Fail = "OR FAIL ",
	Ignore = "OR IGNORE ",
	Replace = "OR REPLACE ",
	Rollback = "OR ROLLBACK "
}

@safe:

string placeholders(size_t x) pure nothrow {
	import std.conv : to;

	if (!x)
		return "";

	auto s = "$1";
	foreach (i; 2 .. x + 1)
		s ~= ",$" ~ i.to!string;
	return s;
}

/** An instance of a query building process */
struct SQLBuilder {
	string sql;
	alias sql this;
	State state;

	this(string sql, State STATE = State.none) {
		this.sql = STATE.startsWithWhite ? sql : STATE ~ sql;
		state = STATE;
	}

	static SB create(T)() if (isAggregateType!T) {
		enum sql = createTable!T;
		return sql;
	}

	///
	unittest {
		assert(SQLBuilder.create!User == `CREATE TABLE IF NOT EXISTS "User"("name" TEXT,"age" INT)`);
		static assert(!__traits(compiles, SQLBuilder.create!int));
	}

	alias insert(T) = insert!(OR.None, T);

	static SB insert(OR or = OR.None, T)() if (isAggregateType!T)
		=> SB(make!(or ~ "INTO " ~ quote(SQLName!T) ~ '(',
				") VALUES(" ~ placeholders(ColumnCount!T) ~ ')', T), State.insert);

	///
	unittest {
		assert(SQLBuilder.insert!User == `INSERT INTO "User"("name","age") VALUES($1,$2)`);
		assert(SQLBuilder.insert!Message == `INSERT INTO "msg"("contents") VALUES($1)`);
	}

	///
	static SB select(Fields...)() if (Fields.length) {
		enum sql = [Fields].join(',');
		return SB(sql, State.select);
	}

	///
	unittest {
		assert(SQLBuilder.select!("only_one") == "SELECT only_one");
		assert(SQLBuilder.select!("hey", "you") == "SELECT hey,you");
	}

	///
	static SB selectAllFrom(Tables...)() if (allSatisfy!(isAggregateType, Tables)) {
		string[] fields, tables;
		foreach (S; Tables) {
			{
				enum tblName = SQLName!S;
				foreach (N; FieldNameTuple!S)
					fields ~= tblName.quote ~ '.' ~ ColumnName!(S, N).quote;

				tables ~= tblName;
			}
		}
		return SB("SELECT " ~ fields.join(',') ~ " FROM "
				~ tables.quoteJoin(), State.from);
	}
	///
	unittest {
		assert(SQLBuilder.selectAllFrom!(Message, User) ==
				`SELECT "msg"."rowid","msg"."contents","User"."name","User"."age" FROM "msg","User"`);
	}

	///
	mixin(Clause!("from", "set", "select"));

	///
	SB from(Tables...)(Tables tables)
	if (Tables.length > 1 && allSatisfy!(isSomeString, Tables))
		=> from([tables].join(','));

	///
	SB from(Tables...)() if (Tables.length && allSatisfy!(isAggregateType, Tables))
		=> from([staticMap!(SQLName, Tables)].quoteJoin());

	///
	mixin(Clause!("set", "update"));

	///
	static SB update(OR or = OR.None, S)(S table) if (isSomeString!S)
		=> SB(or ~ table, State.update);

	///
	static SB update(T, OR or = OR.None)() if (isAggregateType!T)
		=> SB(or ~ SQLName!T, State.update);

	///
	static SB updateAll(T, OR or = OR.None)() if (isAggregateType!T)
		=> SB(make!("UPDATE " ~ or ~ SQLName!T ~ " SET ", "=?", T), State.set);

	///
	unittest {
		assert(SQLBuilder.update("User") == "UPDATE User");
		assert(SQLBuilder.update!User == "UPDATE User");
		assert(SQLBuilder.updateAll!User == `UPDATE User SET "name"=$1,"age"=$2`);
	}

	///
	mixin(Clause!("where", "set", "from", "del"));

	///
	static SB del(Table)() if (isAggregateType!Table)
		=> del(SQLName!Table);

	///
	static SB del(S)(S table) if (isSomeString!S)
		=> SB(table, State.del);

	///
	unittest {
		assert(SQLBuilder.del!User.where("name=$1") ==
				`DELETE FROM User WHERE name=$1`);
	}

	///
	mixin(Clause!("groupBy", "from", "where"));

	///
	mixin(Clause!("having", "from", "where", "groupBy"));

	///
	mixin(Clause!("orderBy", "from", "where", "groupBy", "having"));

	///
	mixin(Clause!("limit", "from", "where", "groupBy", "having", "orderBy"));

	///
	mixin(Clause!("offset", "limit"));

	SB opCall(S)(S expr) if (isSomeString!S) {
		sql ~= expr;
		return this;
	}

private:
	enum Clause(string name, prevStates...) =
		"SB " ~ name ~ "(S)(S expr) if(isSomeString!S)
		in(state == State."
		~ [prevStates].join(
			" || state == State.") ~ `, "Wrong SQL: ` ~ name ~ ` after " ~ state) {
		sql ~= (state = State.`
		~ name ~ ") ~ expr;
		return this;}";

	template make(string prefix, string suffix, T) if (isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, T);
		enum make = sql!sqlFields;
	}
}

///
unittest {
	// This will map to a "User" table in our database
	struct User {
		string name;
		int age;
	}

	assert(SB.create!User == `CREATE TABLE IF NOT EXISTS "User"("name" TEXT,"age" INT)`);

	auto q = SB.select!"name"
		.from!User
		.where("age=$1");

	// The properties `sql` can be used to access the generated sql
	assert(q.sql == `SELECT name FROM "User" WHERE age=$1`);

	/// We can decorate structs and fields to give them different names in the database.
	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}

	// Note that virtual "rowid" field is handled differently -- it will not be created
	// by create(), and not inserted into by insert()

	assert(SB.create!Message == `CREATE TABLE IF NOT EXISTS "msg"("contents" TEXT)`);

	auto q2 = SB.insert!Message;
	assert(q2 == `INSERT INTO "msg"("contents") VALUES($1)`);
}

unittest {
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;

	alias C = ColumnName;

	// Make sure all these generate the same sql statement
	auto sql = [
		SB.select!(`"msg"."rowid"`, `"msg"."contents"`).from(`"msg"`)
			.where(`"msg"."rowid"=$1`).sql,
		SB.select!(`"msg"."rowid"`, `"msg"."contents"`)
			.from!Message
			.where(C!(Message.id) ~ "=$1").sql,
		SB.select!(C!(Message.id), C!(Message.contents))
			.from!Message
			.where(`"msg"."rowid"=$1`).sql,
		SB.selectAllFrom!Message.where(`"msg"."rowid"=$1`).sql
	];
	assert(count(uniq(sql)) == 1);
}

private:

bool startsWithWhite(S)(S s)
	=> s.length && s[0].isWhite;

SB createTable(T)() {
	import std.conv : to;

	string s;
	static foreach (A; __traits(getAttributes, T))
		static if (is(typeof(A)))
			static if (isSomeString!(typeof(A)))
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
					string field = quote(colName) ~ ' ',
					type = SQLTypeOf!(FIELDS[I]),
					constraints;
				}
				static foreach (A; __traits(getAttributes, T.tupleof[I]))
					static if (is(typeof(A) == sqlkey)) {
						static if (A.key.length)
							keys ~= "FOREIGN KEY(" ~ quote(colName) ~ ") REFERENCES " ~ A.key;
						else
							pkeys ~= colName;
					} else static if (colName != "rowid" && is(typeof(A) == sqltype))
						type = A.type;
					else static if (is(typeof(A)))
						static if (isSomeString!(typeof(A)))
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
						field ~= " default " ~ quote(member.to!string, '\'');
					fields ~= field;
				}
			}
		}
	if (pkeys.length)
		keys ~= "PRIMARY KEY(" ~ quoteJoin(pkeys) ~ ')';

	return SB(quote(SQLName!T) ~ '(' ~ join(fields ~ keys, ',') ~ ')'
			~ s, State.createNX);
}

alias SB = SQLBuilder;

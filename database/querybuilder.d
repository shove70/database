module database.querybuilder;

import std.meta;
import std.traits;

import database.sqlbuilder;

/++ Internal wrapper for placeholder marker in generated SQL fragments. +/
struct Placeholder(alias x);

@safe:

/++ Build a DELETE query builder for a table type. +/
alias del(T) = QueryBuilder!(SB.del!T);

/++ Build a SELECT query builder for one or more field/table expressions. +/
alias select(T...) = QueryBuilder!(SB.select!T);

/++ Build an UPDATE query builder for table `T`. +/
alias update(T, OR or = OR.None) = QueryBuilder!(SB.update!(T, or));

/++ A typed, fluent query-builder wrapper around an `SQLBuilder` template. +/
struct QueryBuilder(SB sb, Args...) {
	enum sql = sb.sql;
	alias args = Args;
	alias all = AS!(sql, args);

	/++ Forward clause operators to the underlying SQLBuilder and collect parameters.

	Params:
		key = Clause/method name to dispatch.
	+/
	template opDispatch(string key) {
		template opDispatch(A...) {
			static if (A.length && allSatisfy!(isType, A)) {
				alias T = __traits(getMember, sb, key);
				alias opDispatch = QueryBuilder!(
					__traits(child, sb, T!A)(),
					Args);
			} else {
				alias expr = AS!();
				alias args = AS!();
				static foreach (a; A) {
					static if (is(typeof(&a))) {
						args = NoDuplicates!(args, a);
						expr = AS!(expr, Placeholder!a);
					} else
						expr = AS!(expr, a);
				}

				alias opDispatch = QueryBuilder!(
					__traits(getMember, sb, key)(putPlaceholder!expr(Args.length)),
					Args, args);
			}
		}
	}

	alias all this;
}

 /// End-to-end query builder API examples with `select`, `update`, and `delete`.
unittest {
	import database.util;

	@snakeCase
	struct User {
		@sqlkey() uint id;
		string name;
		uint parent;
	}

	uint id = 1;
	auto name = "name";

	alias s = select!"name".from!User
		.where!("id=", id);
	static assert(s.sql == `SELECT name FROM "user" WHERE id=$1`);
	assert(s.args == AliasSeq!(id));

	alias s2 = select!(User.name).where!("id=", id);
	static assert(s2.sql == `SELECT name FROM "user" WHERE id=$1`);
	assert(s2.args == AliasSeq!(id));

	alias s3 = select!(User.name).where!("id>=", id, " AND parent=", id);
	static assert(s3.sql == `SELECT name FROM "user" WHERE id>=$1 AND parent=$1`);
	assert(s3.args == AliasSeq!(id));

	alias u = update!User.set!("name=", name)
		.where!("id=", id);
	static assert(u.sql == `UPDATE "user" SET name=$1 WHERE id=$2`);
	assert(u.args == AliasSeq!(name, id));

	alias d = del!User.where!("id=", id);
	static assert(d.sql == `DELETE FROM "user" WHERE id=$1`);
	assert(d.args == AliasSeq!(id));
}

private:

alias AS = AliasSeq;

/++ Convert placeholder markers into positional SQL parameters.

Params:
	start = Starting parameter index.
Returns: SQL expression with `$1`, `$2`, ... substitutions.
+/
string putPlaceholder(A...)(uint start) {
	import std.conv : text;

	auto s = "";
	foreach (a; A) {
		static if (isInstanceOf!(Placeholder, a))
			s ~= text('$', start + staticIndexOf!(a, A));
		else
			s ~= text(a);
	}
	return s;
}

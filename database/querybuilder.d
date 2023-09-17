module database.querybuilder;

import std.meta;
import std.traits;

import database.sqlbuilder;
import database.util;

enum Placeholder;

alias del(T) = QueryBuilder!(SB.del!T);

alias select(T...) = QueryBuilder!(SB.select!T);

alias update(T, OR or = OR.None) = QueryBuilder!(SB.update!(T, or));

struct QueryBuilder(SB sb, Args...) {
	enum sql = sb.sql;
	alias args = Args;
	alias all = AS!(sql, args);

	template opDispatch(string key) {
		template opDispatch(A...) {
			static if (A.length && allSatisfy!(isType, A))
				alias opDispatch = QueryBuilder!(
					mixin("sb.", key, "!A")(),
					Args);
			else {
				import std.algorithm : move;

				alias expr = AS!();
				alias args = AS!();
				static foreach (a; A) {
					static if (is(typeof(move(a)))) {
						args = AS!(args, a);
						expr = AS!(expr, Placeholder);
					} else
						expr = AS!(expr, a);
				}

				alias opDispatch = QueryBuilder!(
					__traits(getMember, sb, key)(putPlaceholder!expr(Args.length + 1)),
					Args, args);
			}
		}
	}

	alias all this;
}

@safe unittest {
	@snakeCase
	struct User {
		@sqlkey() uint id;
		string name;
	}

	uint id = 1;
	auto name = "name";

	alias s = Alias!(select!"name".from!User
			.where!("id=", id));
	static assert(s.sql == `SELECT name FROM "user" WHERE id=$1`);
	assert(s.args == AliasSeq!(id));

	alias u = Alias!(update!User.set!("name=", name)
			.from!User
			.where!("id=", id));
	static assert(u.sql == `UPDATE "user" SET name=$1 FROM "user" WHERE id=$2`);
	assert(u.args == AliasSeq!(name, id));

	alias d = Alias!(del!User.where!("id=", id));
	static assert(d.sql == `DELETE FROM "user" WHERE id=$1`);
	assert(d.args == AliasSeq!(id));
}

private:

alias AS = AliasSeq;

string putPlaceholder(T...)(uint start = 1) {
	import std.conv : text;

	auto s = "";
	alias i = start;
	foreach (a; T) {
		static if (is(a == Placeholder))
			s ~= text('$', i++);
		else
			s ~= text(a);
	}
	return s;
}

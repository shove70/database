module database.mysql.row;

import database.mysql.exception;
import database.mysql.type;
public import database.row;

private uint hashOf(const(char)[] x)
{
	import std.ascii;

	uint hash = 2166136261u;
	foreach(i; 0..x.length)
		hash = (hash ^ cast(uint)(toLower(x.ptr[i]))) * 16777619u;

	return hash;
}

alias MySQLRow = Row!(MySQLValue, MySQLHeader, MySQLErrorException, hashOf, Mixin);

private template Mixin()
{
	private static bool equalsCI(const(char)[] x, const(char)[] y)
	{
		import std.ascii;

		if (x.length != y.length)
			return false;

		foreach(i; 0..x.length)
			if (toLower(x.ptr[i]) != toLower(y.ptr[i]))
				return false;

		return true;
	}

	package uint find(uint hash, const(char)[] key) const
	{
		if (auto mask = index_.length - 1) {
			assert((index_.length & mask) == 0);

			hash = hash & mask;
			uint probe;

			for (;;) {
				auto index = index_[hash];
				if (!index)
					break;
				if (equalsCI(_header[index - 1].name, key))
					return index;
				hash = (hash + ++probe) & mask;
			}
		}

		return 0;
	}

	private void structurize(Strict strict = Strict.yesIgnoreNull, string path = null, T)(ref T result)
	{
		import database.mysql.exception;
		import database.mysql.type;
		import database.util;
		import std.format : format;

		enum unCamel = hasUDA!(T, snakeCase);
		foreach(member; __traits(allMembers, T))
		{
			static if (isWritableDataMember!(__traits(getMember, T, member)))
			{
				static if (!hasUDA!(__traits(getMember, result, member), as))
				{
					enum pathMember = path ~ member;
					static if (unCamel)
					{
						enum pathMemberAlt = path ~ member.snakeCase;
					}
				}
				else
				{
					enum pathMember = path ~ getUDAs!(__traits(getMember, result, member), as)[0].name;
					static if (unCamel)
					{
						enum pathMemberAlt = pathMember;
					}
				}

				alias MemberType = typeof(__traits(getMember, result, member));

				static if (isPointer!MemberType && !isValueType!(PointerTarget!MemberType) || !isValueType!MemberType)
				{
					enum pathNew = pathMember ~ '.';
					enum st = Select!(hasUDA!(__traits(getMember, result, member), optional), Strict.no, strict);
					static if (isPointer!MemberType)
					{
						if (__traits(getMember, result, member))
							structurize!(st, pathNew)(*__traits(getMember, result, member));
					}
					else
					{
						structurize!(st, pathNew)(__traits(getMember, result, member));
					}
				}
				else
				{
					enum hash = pathMember.hashOf;
					static if (unCamel)
					{
						enum hashAlt = pathMemberAlt.hashOf;
					}

					auto index = find(hash, pathMember);
					static if (unCamel && (pathMember != pathMemberAlt))
					{
						if (!index)
							index = find(hashAlt, pathMemberAlt);
					}

					if (index)
					{
						auto pvalue = values[index - 1];

						static if (strict == Strict.no || strict == Strict.yesIgnoreNull || hasUDA!(__traits(getMember, result, member), optional))
						{
							if (pvalue.isNull)
								continue;
						}

						__traits(getMember, result, member) = pvalue.get!(Unqual!MemberType);
						continue;
					}

					static if ((strict == Strict.yes || strict == Strict.yesIgnoreNull) && !hasUDA!(__traits(getMember, result, member), optional))
					{
						static if (!unCamel || (pathMember == pathMemberAlt))
						{
							enum ColumnError = "Column '%s' was not found in this result set".format(pathMember);
						}
						else
						{
							enum ColumnError = "Column '%s' or '%s' was not found in this result set".format(pathMember, pathMemberAlt);
						}
						throw new MySQLErrorException(ColumnError);
					}
				}
			}
		}
	}
}

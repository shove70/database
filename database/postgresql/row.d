module database.postgresql.row;

import std.traits;
import std.typecons;
import database.postgresql.exception;
import database.postgresql.type;
public import database.row;
import database.util;

alias PgSQLRow = Row!(PgSQLValue, PgSQLHeader, PgSQLErrorException, hashOf, Mixin);

private template Mixin()
{
	package uint find_(size_t hash, const(char)[] key) const
	{
		if (auto mask = index_.length - 1) {
			assert((index_.length & mask) == 0);

			hash = hash & mask;
			uint probe = 1;

			while (true)
			{
				auto index = index_[hash];
				if (index)
				{
					if (names_[index - 1] == key)
						return index;
					hash = (hash + probe++) & mask;
				}
				else
				{
					break;
				}
			}
		}
		return 0;
	}

	void structurize(Strict strict = Strict.yesIgnoreNull, string path = null, T)(ref T result) {
		import database.postgresql.exception;
		import database.postgresql.type;
		import std.format : format;

		foreach(i, ref member; result.tupleof) {
			static if (isWritableDataMember!member) {
				enum
					colName = path ~ SQLName!(result.tupleof[i]),
					opt = hasUDA!(member, optional) || strict == Strict.no;

				static if (isValueType!(typeof(member))) {
					enum hash = colName.hashOf;

					if (auto index = find(hash, colName)) {
						auto pvalue = values[index - 1];

						static if (strict != Strict.yes || opt) {
							if (pvalue.isNull)
								continue;
						}

						member = pvalue.get!(Unqual!(typeof(member)));
						continue;
					}

					static if (!opt)
						throw new PgSQLErrorException("Column '%s' was not found in this result set".format(colName));
				} else {
					enum mode = opt ? Strict.no : strict;
					structurize!(mode, colName ~ '.')(member);
				}
			}
		}
	}
}

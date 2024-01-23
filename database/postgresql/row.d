module database.postgresql.row;

import database.postgresql.exception;
import database.postgresql.type;
public import database.row;

alias PgSQLRow = Row!(PgSQLValue, PgSQLHeader, PgSQLErrorException, hashOf, Mixin);

private template Mixin() {
	package uint find(size_t hash, string key) const {
		if (auto mask = index_.length - 1) {
			assert((index_.length & mask) == 0);

			hash &= mask;
			uint probe;

			for (;;) {
				auto index = index_[hash];
				if (!index)
					break;
				if (_header[index - 1].name == key)
					return index;
				hash = (hash + ++probe) & mask;
			}
		}
		return 0;
	}

	void structurize(Strict strict = Strict.yesIgnoreNull, string path = null, T)(ref T result) {
		import database.postgresql.exception;
		import database.postgresql.type;

		foreach (i, ref member; result.tupleof) {
			static if (isWritableDataMember!member) {
				enum colName = path ~ SQLName!(result.tupleof[i]),
					opt = hasUDA!(member, optional) || strict == Strict.no;

				static if (isValueType!(typeof(member))) {
					enum hash = colName.hashOf;

					if (const index = find(hash, colName)) {
						const pvalue = values[index - 1];

						static if (strict != Strict.yes || opt) {
							if (pvalue.isNull)
								continue;
						}

						member = pvalue.get!(Unqual!(typeof(member)));
						continue;
					}

					static if (!opt)
						throw new PgSQLErrorException(
							"Column '" ~ colName ~ "' was not found in this result set");
				} else
					structurize!(opt ? Strict.no : strict, colName ~ '.')(member);
			}
		}
	}
}

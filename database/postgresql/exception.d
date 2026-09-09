module database.postgresql.exception;

import database.util : DBException;

@safe:

/++ Base exception for PostgreSQL runtime and driver errors. +/
class PgSQLException : DBException {
	/++ Base PostgreSQL exception for library/runtime issues. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLConnectionException : PgSQLException {
	/++ Raised when opening or using a PostgreSQL socket/connection fails. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLProtocolException : PgSQLException {
	/++ Raised when the protocol message sequence is malformed or unexpected. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLErrorException : DBException {
	/++ Represents an error reported by the PostgreSQL server. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLDuplicateEntryException : PgSQLErrorException {
	/++ Raised when a duplicate key or unique index violation occurs. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

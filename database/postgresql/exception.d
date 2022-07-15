module database.postgresql.exception;

class PgSQLException : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLConnectionException : PgSQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLProtocolException : PgSQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLErrorException : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class PgSQLDuplicateEntryException : PgSQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

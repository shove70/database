module database.mysql.exception;

import database.util : DBException;

@safe pure:

class MySQLException : DBException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLConnectionException : MySQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLProtocolException : MySQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLErrorException : DBException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLDuplicateEntryException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLDataTooLongException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLDeadlockFoundException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLTableDoesntExistException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLLockWaitTimeoutException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

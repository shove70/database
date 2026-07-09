module database.mysql.exception;

import database.util : DBException;

@safe:

/++ Base MySQL exception type for protocol-level and runtime failures. +/
class MySQLException : DBException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Raised when the client fails to connect or a connection is terminated. +/
class MySQLConnectionException : MySQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLProtocolException : MySQLException {
	/++ Raised when wire protocol parsing or protocol state is invalid. +/
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Base type for server-reported MySQL errors. +/
class MySQLErrorException : DBException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Raised when a duplicate key/entry error is reported by the server. +/
class MySQLDuplicateEntryException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Raised when a value exceeds the target column capacity. +/
class MySQLDataTooLongException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Raised when a deadlock is detected while processing a transaction. +/
class MySQLDeadlockFoundException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Raised when a statement references a non-existing table. +/
class MySQLTableDoesntExistException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

/++ Raised when a lock wait times out. +/
class MySQLLockWaitTimeoutException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

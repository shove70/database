module database.postgresql.protocol;

import std.datetime;

//dfmt off

//https://www.postgresql.org/docs/14/static/protocol-message-formats.html
/++ Message type byte sent by client to PostgreSQL in the output protocol stream. +/
enum OutputMessageType : ubyte {
	Bind			= 'B',
	Close			= 'C',
	CopyData		= 'd',
	CopyDone		= 'c',
	CopyFail		= 'f',
	Describe		= 'D',
	Execute			= 'E',
	Flush			= 'H',
	FunctionCall	= 'F',
	Parse			= 'P',
	PasswordMessage = 'p',
	Query			= 'Q',
	Sync			= 'S',
	Terminate		= 'X'
}

/++ Message type byte received from PostgreSQL in the input protocol stream. +/
enum InputMessageType : ubyte {
	Authentication		= 'R',
	BackendKeyData		= 'K',
	BindComplete		= '2',
	CloseComplete		= '3',
	CommandComplete		= 'C',
	CopyData			= 'd',
	CopyDone			= 'c',
	CopyInResponse		= 'G',
	CopyOutResponse		= 'H',
	CopyBothResponse	= 'W',
	DataRow				= 'D',
	EmptyQueryResponse	= 'I',
	ErrorResponse		= 'E',
	FunctionCallResponse= 'V',
	NoData				= 'n',
	NoticeResponse		= 'N',
	NotificationResponse= 'A',
	ParameterDescription= 't',
	ParameterStatus		= 'S',
	ParseComplete		= '1',
	PortalSuspended		= 's',
	ReadyForQuery		= 'Z',
	RowDescription		= 'T'
}

/++ Extended query transaction status flag reported by PostgreSQL. +/
enum TransactionStatus : ubyte {
	Idle = 'I',
	Inside = 'T',
	Error = 'E',
}

/++ Identifies whether a PostgreSQL command refers to a statement or a portal. +/
enum DescribeType : ubyte {
	Statement = 'S',
	Portal = 'P'
}

/++ Binary/text data layout code used in Bind/RowDescription messages. +/
enum FormatCode : short {
	Text,
	Binary
}

// https://www.postgresql.org/docs/14/static/protocol-error-fields.html
/++ Error-field identifier used in PostgreSQL notice/error responses. +/
enum NoticeMessageField : ubyte {
	SeverityLocal		= 'S',
	Severity			= 'V',
	Code				= 'C',
	Message				= 'M',
	Detail				= 'D',
	Hint				= 'H',
	Position			= 'P',
	InternalPosition	= 'p',
	InternalQuery		= 'q',
	Where				= 'W',
	Schema				= 's',
	Table				= 't',
	Column				= 'c',
	DataType			= 'd',
	Constraint			= 'n',
	File				= 'F',
	Line				= 'L',
	Routine				= 'R',
}

// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
/++ OID-backed PostgreSQL type identifiers used by parameter and row encoding. +/
enum PgType : uint {
	NULL		= 0,
	BOOL		= 16,
	BYTEA		= 17,
	CHAR		= 18,
	NAME		= 19,
	INT8		= 20,
	INT2		= 21,
	// INTVEC2	= 22,
	INT4		= 23,
	// REGPROC	= 24,
	TEXT		= 25,
	OID			= 26,
	// TID		= 27,
	// XID		= 28,
	// CID		= 29,
	// OIDARRAY	= 30,

	// PG_TYPE	= 71,
	// PG_ATTRIBUTE = 75,
	// PG_PROC		= 81,
	// PG_CLASS	= 83,

	JSON		= 114,
	XML			= 142,

	POINT		= 600,
	LSEG		= 601,
	PATH		= 602,
	BOX			= 603,
	POLYGON		= 604,
	LINE		= 628,

	REAL		= 700,
	DOUBLE		= 701,
	// ABSTIME	= 702,
	// RELTIME	= 703,
	TINTERVAL	= 704,
	UNKNOWN		= 705,
	CIRCLE		= 718,
	MONEY		= 790,

	MACADDR		= 829,
	INET		= 869,
	CIDR		= 650,
	MACADDR8	= 774,

	CHARA		= 1042,
	VARCHAR		= 1043,
	DATE		= 1082,
	TIME		= 1083,

	TIMESTAMP	= 1114,
	TIMESTAMPTZ	= 1184,
	INTERVAL	= 1186,

	TIMETZ		= 1266,

	BIT			= 1560,
	VARBIT		= 1562,

	NUMERIC	= 1700,
	// REFCURSOR	= 1790,

	// REGPROCEDURE	= 2202,
	// REGOPER		= 2203,
	// REGOPERATOR 	= 2204,
	// REGCLASS		= 2205,
	// REGTYPE		= 2206,
	// REGROLE		= 4096,
	// REGNAMESPACE	= 4089,
	VOID		= 2278,
	UUID		= 2950,
	JSONB		= 3802
}

alias PgColumnTypes = PgType;

/++ PostgreSQL epoch reference date used for date conversions. +/
enum PGEpochDate = Date(2000, 1, 1);
enum PGEpochDay = PGEpochDate.dayOfGregorianCal;
enum PGEpochTime = TimeOfDay(0, 0, 0);
enum PGEpochDateTime = DateTime(2000, 1, 1, 0, 0, 0);

/++ Map a `PgType` code to a human-readable SQL type name. +/
auto columnTypeName(PgType type) {
	import std.traits;

	final switch (type) {
		case PgType.DOUBLE: return "double precision";
		case PgType.CHARA: return "char(n)";
		static foreach(M; EnumMembers!PgType) {
			static if(M != PgType.DOUBLE && M != PgType.CHARA)
				case M: return M.stringof;
		}
	}
}

struct Notification {
	/++ Process ID of the backend that emitted the notification. +/
	int processId;
	/++ Notification channel name. +/
	char[] channel;
	/++ Notification message payload. +/
	char[] payload;
}

/++ PostgreSQL notice/error information sent by the server. +/
struct Notice {
	enum Severity : ubyte {
		ERROR = 1,
		FATAL,
		PANIC,
		WARNING,
		NOTICE,
		DEBUG,
		INFO,
		LOG,
	}

	/++ Severity of the notice or error response. +/
	Severity severity;
	/++ SQL error code from `SQLSTATE` (or empty if not available). +/
	char[5] code;
	/++ Primary human-readable error message; usually one concise line. +/
	string message;
	/++ Optional secondary detail message with additional context. +/
	string detail;
	/++ Optional suggestion about what to do about the problem. +/
	string hint;
	/++ Decimal ASCII integer cursor position in the query string. +/
	uint position;
	/++ Cursor position for internally generated commands, if available. +/
	string internalPos;
	/++ Text of a failed internally-generated command. +/
	string internalQuery;
	/++ Context where the error occurred, including routine stack trace. +/
	string where;
	/++ Database schema referenced by the notice/error, if applicable. +/
	string schema;
	/++ Database table referenced by the notice/error, if applicable. +/
	string table;
	/++ Database column referenced by the notice/error, if applicable. +/
	string column;
	/++ Name of the related SQL type, if any. +/
	string type;
	/++ Constraint name related to the error, if applicable. +/
	string constraint;
	/++ Source file name where the error was reported. +/
	string file;
	/++ Source line number where the error was reported. +/
	string line;
	/++ Routine name that reported the error. +/
	string routine;

	string toString() @safe const {
		import std.array;

		auto writer = appender!string;
		toString(writer);
		return writer[];
	}

	void toString(W)(ref W writer) const {
		import std.format : formattedWrite;
		writer.formattedWrite("%s(%s) %s", severity, code, message);
	}
}

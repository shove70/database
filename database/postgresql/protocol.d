module database.postgresql.protocol;

import std.datetime;

//dfmt off

//https://www.postgresql.org/docs/14/static/protocol-message-formats.html
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

enum TransactionStatus : ubyte {
	Idle = 'I',
	Inside = 'T',
	Error = 'E',
}

enum DescribeType : ubyte {
	Statement = 'S',
	Portal = 'P'
}

enum FormatCode : short {
	Text,
	Binary
}

// https://www.postgresql.org/docs/14/static/protocol-error-fields.html
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

enum PGEpochDate = Date(2000, 1, 1);
enum PGEpochDay = PGEpochDate.dayOfGregorianCal;
enum PGEpochTime = TimeOfDay(0, 0, 0);
enum PGEpochDateTime = DateTime(2000, 1, 1, 0, 0, 0);

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
	/// The process ID of the notifying backend process
	int processId;
	/// The name of the channel that the notify has been raised on
	char[] channel;
	/// The “payload” string passed from the notifying process
	char[] payload;
}

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

	Severity severity;
	/// https://www.postgresql.org/docs/14/static/errcodes-appendix.html
	char[5] code;
	/// Primary human-readable error message. This should be accurate
	/// but terse (typically one line). Always present.
	string message;
	/// Optional secondary error message carrying more detail about the
	/// problem. Might run to multiple lines.
	string detail;
	/** Optional suggestion what to do about the problem. This is intended to
	differ from Detail in that it offers advice (potentially inappropriate)
	rather than hard facts. Might run to multiple lines. */
	string hint;
	/** Decimal ASCII integer, indicating an error cursor position as an index
	into the original query string. The first character has index 1, and
	positions are measured in characters not bytes. */
	uint position;
	/** this is defined the same as the position field, but it is used when
	the cursor position refers to an internally generated command rather than
	the one submitted by the client. The q field will always appear when this
	field appears.*/
	string internalPos;
	/// Text of a failed internally-generated command. This could be, for
	/// example, a SQL query issued by a PL/pgSQL function.
	string internalQuery;
	/** Context in which the error occurred. Presently this includes a call
	stack traceback of active procedural language functions and
	internally-generated queries. The trace is one entry per line, most
	recent first. */
	string where;
	string schema;
	string table;
	string column;
	/// If the error was associated with a specific data type, the name of
	/// the data type.
	string type;
	/** If the error was associated with a specific constraint, the name of the
	constraint. Refer to fields listed above for the associated table or domain.
	(For this purpose, indexes are treated as constraints, even if they weren't
	created with constraint syntax.) */
	string constraint;
	/// File name of the source-code location where the error was reported.
	string file;
	/// Line number of the source-code location where the error was reported.
	string line;
	/// Name of the source-code routine reporting the error.
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

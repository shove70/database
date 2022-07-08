module database.postgresql.inserter;

public import database.inserter;
import database.postgresql.appender;
import database.postgresql.connection;
import database.postgresql.exception;
import database.postgresql.type;

alias Inserter = DBInserter!(Connection, PgSQLErrorException, '"', isValueType, appendValue);

mixin InserterHelpers!(Connection, Inserter);

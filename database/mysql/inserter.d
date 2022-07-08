module database.mysql.inserter;

public import database.inserter;
import database.mysql.appender;
import database.mysql.connection;
import database.mysql.exception;
import database.mysql.type;

alias Inserter = DBInserter!(Connection, MySQLErrorException, '`', isValueType, appendValue);

mixin InserterHelpers!(Connection, Inserter);

module database.mysql.pool;

import database.mysql.connection;
import database.mysql.protocol;
import database.pool;

alias ConnectionPool = shared ConnectionProvider!(Connection, DefaultClientCaps);

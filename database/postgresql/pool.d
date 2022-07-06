module database.postgresql.pool;

import database.pool;
import database.postgresql.connection;

alias ConnectionPool = shared ConnectionProvider!(Connection, 0);

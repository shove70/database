module database.postgresql.pool;

import database.pool;
import database.postgresql.connection;

/++ Shared connection provider alias backed by `database.pool.ConnectionProvider`. +/
alias ConnectionPool = shared ConnectionProvider!(Connection, 0);

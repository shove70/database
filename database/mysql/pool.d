module database.mysql.pool;

import database.mysql.connection;
import database.mysql.protocol;
import database.pool;

/++ Convenience alias for a shared connection provider bound to MySQL `Connection`.

The provider reuses the generic `ConnectionProvider` with MySQL capability
defaults and exposes pooled connection lifecycle management.
+/
alias ConnectionPool = shared ConnectionProvider!(Connection, DefaultClientCaps);

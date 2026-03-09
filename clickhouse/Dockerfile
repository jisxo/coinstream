FROM clickhouse/clickhouse-server:24.6

# Initialize databases/tables on first boot.
COPY clickhouse/init.sql /docker-entrypoint-initdb.d/init.sql
COPY clickhouse/users.d/default-user.xml /etc/clickhouse-server/users.d/default-user.xml

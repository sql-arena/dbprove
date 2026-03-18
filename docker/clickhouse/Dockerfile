FROM clickhouse/clickhouse-server:26.1

COPY conf/users.d/default-password.xml /etc/clickhouse-server/users.d/default-password.xml

# Expose ClickHouse ports (HTTP, Native Protocol)
EXPOSE 8123 9000

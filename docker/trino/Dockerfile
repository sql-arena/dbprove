FROM trinodb/trino:latest

COPY catalog/tpch.properties /etc/trino/catalog/tpch.properties
COPY bootstrap.sql /opt/trino/bootstrap.sql
COPY --chmod=755 entrypoint.sh /usr/local/bin/trino-entrypoint.sh

# Expose Trino port (Web UI and API)
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/trino-entrypoint.sh"]

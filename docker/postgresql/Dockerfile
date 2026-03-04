FROM postgres:latest

# Copy tuned benchmark configuration and init script
COPY conf/benchmark.conf /etc/postgresql/benchmark.conf
COPY init/00-enable-benchmark-conf.sh /docker-entrypoint-initdb.d/00-enable-benchmark-conf.sh

# Expose default PostgreSQL port
EXPOSE 5432

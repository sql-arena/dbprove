FROM postgres:18

COPY conf/benchmark.conf /etc/postgresql/benchmark.conf
COPY --chmod=755 init/00-enable-benchmark-conf.sh /docker-entrypoint-initdb.d/00-enable-benchmark-conf.sh

EXPOSE 5432

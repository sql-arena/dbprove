FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PGDATA=/var/lib/postgresql/data

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        postgresql \
        postgresql-contrib \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY conf/benchmark.conf /etc/postgresql/benchmark.conf
COPY --chmod=755 entrypoint.sh /usr/local/bin/postgresql-entrypoint.sh

# Expose default PostgreSQL port
EXPOSE 5432

ENTRYPOINT ["/usr/local/bin/postgresql-entrypoint.sh"]

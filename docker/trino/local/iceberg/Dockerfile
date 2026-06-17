FROM eclipse-temurin:25-jre-jammy AS trino-java

FROM trinodb/trino:latest AS trino-dist

FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        less \
        libstdc++6 \
        procps \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd trino --gid 1000 \
    && useradd trino --uid 1000 --gid 1000 --create-home

ENV JAVA_HOME=/opt/trino-java
ENV PATH=${JAVA_HOME}/bin:${PATH}
ENV LANG=C.UTF-8
ENV CATALOG_MANAGEMENT=static

COPY --from=trino-java /opt/java/openjdk /opt/trino-java
COPY --from=trino-dist /usr/bin/trino /usr/bin/trino
COPY --from=trino-dist /usr/lib/trino /usr/lib/trino

RUN mkdir -p /data/trino /opt/trino \
    && chown -R trino:trino /usr/lib/trino /data/trino /opt/trino

COPY config.properties /etc/trino/config.properties
COPY jvm.config /etc/trino/jvm.config
COPY catalog/tpch.properties /etc/trino/catalog/tpch.properties
COPY bootstrap.sql /opt/trino/bootstrap.sql
COPY --chmod=755 entrypoint.sh /usr/local/bin/trino-entrypoint.sh

# Expose Trino port (Web UI and API)
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/trino-entrypoint.sh"]

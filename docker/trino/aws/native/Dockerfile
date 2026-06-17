ARG DBPROVE_PREBUILT_IMAGE=dbprove-prebuilt:latest

FROM apache/hive:4.0.1 AS hive-metastore

FROM eclipse-temurin:25-jre-noble AS trino-java

FROM trinodb/trino:latest AS trino-dist

FROM ${DBPROVE_PREBUILT_IMAGE} AS dbprove-prebuilt

FROM ubuntu:22.04

USER root

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
ENV PATH=/opt/dbprove/bin:${JAVA_HOME}/bin:${PATH}
ENV LANG=C.UTF-8
ENV CATALOG_MANAGEMENT=static

COPY --from=trino-java /opt/java/openjdk /opt/trino-java
COPY --from=hive-metastore /opt/java/openjdk /opt/hive-java
COPY --from=hive-metastore /opt/hive /opt/hive
COPY --from=hive-metastore /opt/tez /opt/tez
COPY --from=trino-dist /usr/bin/trino /usr/bin/trino
COPY --from=trino-dist /usr/lib/trino /usr/lib/trino

RUN mkdir -p /opt/dbprove/bin /mnt/nvme /data/trino /opt/hive/conf \
    && chown -R trino:trino /usr/lib/trino /data/trino /opt/dbprove

COPY --from=dbprove-prebuilt /opt/dbprove /opt/dbprove

COPY docker/trino/aws/native/config.properties /etc/trino/config.properties
COPY docker/trino/aws/native/jvm.config /etc/trino/jvm.config
COPY docker/trino/aws/native/hive-site.xml /opt/hive/conf/hive-site.xml
COPY docker/trino/aws/native/catalog/native.properties /etc/trino/catalog/native.properties
COPY --chmod=755 docker/trino/aws/native/entrypoint.sh /usr/local/bin/trino-entrypoint.sh

RUN chmod +x /opt/dbprove/bin/dbprove

EXPOSE 8080
EXPOSE 9083

ENTRYPOINT ["/usr/local/bin/trino-entrypoint.sh"]

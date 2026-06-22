FROM eclipse-temurin:25-jre-jammy AS trino-java

FROM trinodb/trino:latest AS trino-dist

FROM ghcr.io/projectnessie/nessie:latest

USER root

RUN microdnf install -y \
      bash \
      ca-certificates \
      python3 \
    && microdnf clean all

COPY --from=trino-dist /usr/bin/trino /usr/bin/trino
COPY --from=trino-dist /usr/lib/trino /usr/lib/trino
COPY --from=trino-java /opt/java/openjdk /opt/trino-java

RUN rm -rf /usr/lib/trino/var \
    && find /usr/lib/trino/plugin -mindepth 1 -maxdepth 1 ! -name iceberg -exec rm -rf {} + \
    && mkdir -p /etc/trino/catalog /data/trino/spill /data/trino/var/run /opt/dbprove/warehouse \
    && ln -s /data/trino/var /usr/lib/trino/var \
    && chown -R 10000:0 /etc/trino /data/trino /opt/dbprove /usr/lib/trino

COPY --chmod=755 entrypoint.sh /usr/local/bin/iceberg-entrypoint.sh
COPY iceberg_wrapper.py /opt/dbprove/iceberg_wrapper.py
COPY trino/config.properties /etc/trino/config.properties
COPY trino/jvm.config /etc/trino/jvm.config
COPY trino/node.properties /etc/trino/node.properties
COPY trino/catalog/tpch.properties /etc/trino/catalog/tpch.properties

ENV TRINO_SERVER_URL=http://127.0.0.1:8080
ENV TRINO_CATALOG=tpch
ENV TRINO_SCHEMA=default
ENV TRINO_USER=dbprove
ENV ICEBERG_WRAPPER_PORT=19130
ENV CATALOG_MANAGEMENT=static

EXPOSE 8080
EXPOSE 19130

USER 10000

ENTRYPOINT ["/usr/local/bin/iceberg-entrypoint.sh"]

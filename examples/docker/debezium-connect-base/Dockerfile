FROM quay.io/debezium/connect-base:3.7.0.Final

USER root

ARG SCYLLA_CDC_VERSION=2.1.0
ENV SCYLLA_CDC_VERSION=${SCYLLA_CDC_VERSION}

RUN curl -fSL -o /tmp/scylla-cdc-source-connector.zip \
        "https://github.com/scylladb/scylla-cdc-source-connector/releases/download/v${SCYLLA_CDC_VERSION}/scylladb-scylla-cdc-source-connector-${SCYLLA_CDC_VERSION}.zip" && \
    unzip -q /tmp/scylla-cdc-source-connector.zip -d "${KAFKA_CONNECT_PLUGINS_DIR}" && \
    mv "${KAFKA_CONNECT_PLUGINS_DIR}/scylladb-scylla-cdc-source-connector-${SCYLLA_CDC_VERSION}" \
        "${KAFKA_CONNECT_PLUGINS_DIR}/scylla-cdc-source-connector" && \
    rm -f /tmp/scylla-cdc-source-connector.zip "${KAFKA_HOME}/config/log4j.properties" && \
    chown -R kafka:kafka "${KAFKA_CONNECT_PLUGINS_DIR}/scylla-cdc-source-connector"

USER kafka

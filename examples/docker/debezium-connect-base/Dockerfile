FROM quay.io/debezium/connect-base:2.7.4.Final

USER root

ARG SCYLLA_CDC_VERSION=2.0.8
ARG DEBEZIUM_VERSION=2.7.4.Final
ENV SCYLLA_CDC_VERSION=${SCYLLA_CDC_VERSION}

# The Scylla connector does not expose Debezium converters, so remove the converter
# metadata that Kafka Connect would otherwise initialize during plugin discovery.
RUN curl -fSL -o /tmp/scylla-cdc-source-connector.zip \
        "https://github.com/scylladb/scylla-cdc-source-connector/releases/download/v${SCYLLA_CDC_VERSION}/scylladb-scylla-cdc-source-connector-${SCYLLA_CDC_VERSION}.zip" && \
    unzip -q /tmp/scylla-cdc-source-connector.zip -d "${KAFKA_CONNECT_PLUGINS_DIR}" && \
    mv "${KAFKA_CONNECT_PLUGINS_DIR}/scylladb-scylla-cdc-source-connector-${SCYLLA_CDC_VERSION}" \
        "${KAFKA_CONNECT_PLUGINS_DIR}/scylla-cdc-source-connector" && \
    zip -q -d "${KAFKA_CONNECT_PLUGINS_DIR}/scylla-cdc-source-connector/lib/debezium-core-${DEBEZIUM_VERSION}.jar" \
        META-INF/services/org.apache.kafka.connect.storage.Converter \
        'io/debezium/converters/CloudEventsConverter*.class' && \
    rm -f /tmp/scylla-cdc-source-connector.zip "${KAFKA_HOME}/config/log4j.properties" && \
    chown -R kafka:kafka "${KAFKA_CONNECT_PLUGINS_DIR}/scylla-cdc-source-connector"

USER kafka

package com.scylladb.cdc.debezium.connector;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

public class ScyllaErrorHandler extends ErrorHandler {
    public ScyllaErrorHandler(ScyllaConnectorConfig scyllaConnectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(ScyllaConnector.class, scyllaConnectorConfig, queue, replacedErrorHandler);
    }
}

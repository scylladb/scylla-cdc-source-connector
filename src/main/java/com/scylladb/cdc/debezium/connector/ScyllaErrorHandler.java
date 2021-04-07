package com.scylladb.cdc.debezium.connector;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

public class ScyllaErrorHandler extends ErrorHandler {
    public ScyllaErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(ScyllaConnector.class, logicalName, queue);
    }
}

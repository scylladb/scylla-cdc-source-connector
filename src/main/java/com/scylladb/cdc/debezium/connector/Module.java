package com.scylladb.cdc.debezium.connector;

import io.debezium.util.IoUtil;
import java.util.Properties;

public final class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "com/scylladb/cdc/debezium/connector/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "scylla";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "Scylla";
    }
}

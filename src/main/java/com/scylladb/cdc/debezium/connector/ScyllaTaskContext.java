package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.spi.schema.DataCollectionId;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Supplier;

public class ScyllaTaskContext extends CdcSourceTaskContext {

    private final List<Pair<TaskId, SortedSet<StreamId>>> tasks;

    public ScyllaTaskContext(Configuration config, List<Pair<TaskId, SortedSet<StreamId>>> tasks) {
        super(Module.contextName(), config.getString(CommonConnectorConfig.TOPIC_PREFIX), new ScyllaConnectorConfig(config).getCustomMetricTags(), Collections::emptySet);
        this.tasks = tasks;
    }

    public List<Pair<TaskId, SortedSet<StreamId>>> getTasks() {
        return tasks;
    }
}

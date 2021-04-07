package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ScyllaConnectorTask extends BaseSourceTask {

    private static final String CONTEXT_NAME = "scylla-connector-task";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile ScyllaSchema schema;
    private volatile ScyllaTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ErrorHandler errorHandler;

    @Override
    protected ChangeEventSourceCoordinator start(Configuration configuration) {
        final String logicalName = configuration.getString(ScyllaConnectorConfig.LOGICAL_NAME);

        final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(configuration);
        final TopicSelector<CollectionId> topicSelector = ScyllaTopicSelector.defaultSelector(logicalName, connectorConfig.getHeartbeatTopicsPrefix());

        final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
        this.schema = new ScyllaSchema(connectorConfig, structSchema);

        List<Pair<TaskId, SortedSet<StreamId>>> tasks = getTasks(configuration);

        this.taskContext = new ScyllaTaskContext(configuration, tasks);

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        final ScyllaEventMetadataProvider metadataProvider = new ScyllaEventMetadataProvider();

        final EventDispatcher<CollectionId> dispatcher = new EventDispatcher<CollectionId>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                (id) -> true,
                DataChangeEvent::new,
                new ScyllaInconsistentSchemaHandler(),
                metadataProvider,
                null,
                SchemaNameAdjuster.create(logger)
        );

        final ScyllaOffsetContext previousOffsets = getPreviousOffsets(connectorConfig, tasks);

        this.errorHandler = new ScyllaErrorHandler(logicalName, queue);

        final Clock clock = Clock.system();

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffsets,
                errorHandler,
                ScyllaConnector.class,
                connectorConfig,
                new ScyllaChangeEventSourceFactory(connectorConfig, taskContext, schema, dispatcher, clock),
                new DefaultChangeEventSourceMetricsFactory(),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private List<Pair<TaskId, SortedSet<StreamId>>> getTasks(Configuration configuration) {
        String serializedTasks = configuration.getString(ScyllaConnectorConfig.WORKER_CONFIG);
        return Arrays.stream(serializedTasks.split("\n"))
                .map(ConfigSerializerUtil::deserializeTaskConfig).collect(Collectors.toList());
    }

    private ScyllaOffsetContext getPreviousOffsets(ScyllaConnectorConfig connectorConfig, List<Pair<TaskId, SortedSet<StreamId>>> tasks) {
        Map<TaskId, SourceInfo> sourceInfos = new HashMap<>();
        tasks.forEach(t -> {
            TaskId taskId = t.getLeft();
            SourceInfo sourceInfo = new SourceInfo(connectorConfig, taskId);
            sourceInfos.put(taskId, sourceInfo);
        });

        List<Map<String, String>> partitions = sourceInfos.values()
                .stream().map(SourceInfo::partition)
                .collect(Collectors.toList());
        Map<Map<String, String>, Map<String, Object>> offsetMap = context.offsetStorageReader().offsets(partitions);

        sourceInfos.values().forEach(sourceInfo -> {
            Map<String, String> partition = sourceInfo.partition();
            Map<String, Object> offset = offsetMap.get(partition);

            if (offset != null) {
                Timestamp windowStart = new Timestamp(new Date(UUIDs.unixTimestamp(UUID.fromString((String) offset.get(SourceInfo.WINDOW_START)))));
                Timestamp windowEnd = new Timestamp(new Date(UUIDs.unixTimestamp(UUID.fromString((String) offset.get(SourceInfo.WINDOW_END)))));
                Optional<ChangeId> changeId = Optional.empty();
                if (offset.containsKey(SourceInfo.CHANGE_ID_STREAM_ID) && offset.containsKey(SourceInfo.CHANGE_ID_TIME)) {
                    StreamId streamId = new StreamId(Bytes.fromHexString((String) offset.get(SourceInfo.CHANGE_ID_STREAM_ID)));
                    UUID time = UUID.fromString((String) offset.get(SourceInfo.CHANGE_ID_TIME));
                    changeId = Optional.of(new ChangeId(streamId, new ChangeTime(time)));
                }
                TaskState taskState = new TaskState(windowStart, windowEnd, changeId);
                sourceInfo.setTaskState(taskState);
            }
        });
        return new ScyllaOffsetContext(sourceInfos, new TransactionContext());
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return ScyllaConnectorConfig.ALL_FIELDS;
    }

    @Override
    public String version() {
        return Module.version();
    }
}

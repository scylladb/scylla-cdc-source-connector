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
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaConnectorTask extends BaseSourceTask<ScyllaPartition, ScyllaOffsetContext> {
  static {
    // Route Flogger logs from scylla-cdc-java library
    // to log4j.
    System.setProperty(
        "flogger.backend_factory",
        "com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance");
  }

  private static final String CONTEXT_NAME = "scylla-connector-task";

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private volatile ScyllaSchema schema;
  private volatile ScyllaTaskContext taskContext;
  private volatile ChangeEventQueue<DataChangeEvent> queue;
  private volatile ErrorHandler errorHandler;

  @Override
  protected ChangeEventSourceCoordinator<ScyllaPartition, ScyllaOffsetContext> start(
      Configuration configuration) {
    final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(configuration);
    final TopicNamingStrategy topicNamingStrategy =
        DefaultTopicNamingStrategy.create(connectorConfig);

    final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
    this.schema = new ScyllaSchema(connectorConfig, structSchema);

    List<Pair<TaskId, SortedSet<StreamId>>> tasks = getTasks(configuration);

    this.taskContext = new ScyllaTaskContext(configuration, tasks);

    this.queue =
        new ChangeEventQueue.Builder<DataChangeEvent>()
            .pollInterval(connectorConfig.getPollInterval())
            .maxBatchSize(connectorConfig.getMaxBatchSize())
            .maxQueueSize(connectorConfig.getMaxQueueSize())
            .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
            .build();

    final ScyllaEventMetadataProvider metadataProvider = new ScyllaEventMetadataProvider();

    final ScyllaOffsetContext previousOffsets = getPreviousOffsets(connectorConfig, tasks);

    SignalProcessor<ScyllaPartition, ScyllaOffsetContext> signalProcessor =
        new SignalProcessor<>(
            ScyllaConnector.class,
            connectorConfig,
            Map.of(),
            getAvailableSignalChannels(),
            DocumentReader.defaultReader(),
            Offsets.of(new ScyllaPartition(null, null), previousOffsets));

    this.errorHandler = new ScyllaErrorHandler(connectorConfig, queue, errorHandler);

    final EventDispatcher<ScyllaPartition, CollectionId> dispatcher =
        new EventDispatcher<ScyllaPartition, CollectionId>(
            connectorConfig,
            topicNamingStrategy,
            schema,
            queue,
            (id) -> true,
            DataChangeEvent::new,
            new ScyllaInconsistentSchemaHandler(),
            metadataProvider,
            connectorConfig.createHeartbeat(
                topicNamingStrategy, connectorConfig.schemaNameAdjuster(), null, null),
            SchemaNameAdjuster.create(),
            signalProcessor);

    final Clock clock = Clock.system();

    NotificationService<ScyllaPartition, ScyllaOffsetContext> notificationService =
        new NotificationService<>(
            getNotificationChannels(),
            connectorConfig,
            SchemaFactory.get(),
            dispatcher::enqueueNotification);

    final SnapshotterService snapshotterService =
        connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

    ChangeEventSourceCoordinator<ScyllaPartition, ScyllaOffsetContext> coordinator =
        new ChangeEventSourceCoordinator<>(
            Offsets.of(new ScyllaPartition(null, null), previousOffsets),
            errorHandler,
            ScyllaConnector.class,
            connectorConfig,
            new ScyllaChangeEventSourceFactory(
                connectorConfig, taskContext, schema, dispatcher, clock),
            new DefaultChangeEventSourceMetricsFactory<>(),
            dispatcher,
            schema,
            signalProcessor,
            notificationService,
            snapshotterService);

    coordinator.start(taskContext, this.queue, metadataProvider);

    return coordinator;
  }

  private List<Pair<TaskId, SortedSet<StreamId>>> getTasks(Configuration configuration) {
    String serializedTasks = configuration.getString(ScyllaConnectorConfig.WORKER_CONFIG);
    return Arrays.stream(serializedTasks.split("\n"))
        .map(ConfigSerializerUtil::deserializeTaskConfig)
        .collect(Collectors.toList());
  }

  private ScyllaOffsetContext getPreviousOffsets(
      ScyllaConnectorConfig connectorConfig, List<Pair<TaskId, SortedSet<StreamId>>> tasks) {
    Map<TaskId, SourceInfo> sourceInfos = new HashMap<>();
    tasks.forEach(
        t -> {
          TaskId taskId = t.getLeft();
          SourceInfo sourceInfo = new SourceInfo(connectorConfig, taskId);
          sourceInfos.put(taskId, sourceInfo);
        });

    List<Map<String, String>> partitions =
        sourceInfos.values().stream().map(SourceInfo::partition).collect(Collectors.toList());
    Map<Map<String, String>, Map<String, Object>> offsetMap =
        context.offsetStorageReader().offsets(partitions);

    sourceInfos
        .values()
        .forEach(
            sourceInfo -> {
              Map<String, String> partition = sourceInfo.partition();
              Map<String, Object> offset = offsetMap.get(partition);

              if (offset != null) {
                Timestamp windowStart =
                    new Timestamp(
                        new Date(
                            UUIDs.unixTimestamp(
                                UUID.fromString((String) offset.get(SourceInfo.WINDOW_START)))));
                Timestamp windowEnd =
                    new Timestamp(
                        new Date(
                            UUIDs.unixTimestamp(
                                UUID.fromString((String) offset.get(SourceInfo.WINDOW_END)))));
                Optional<ChangeId> changeId = Optional.empty();
                if (offset.containsKey(SourceInfo.CHANGE_ID_STREAM_ID)
                    && offset.containsKey(SourceInfo.CHANGE_ID_TIME)) {
                  StreamId streamId =
                      new StreamId(
                          Bytes.fromHexString((String) offset.get(SourceInfo.CHANGE_ID_STREAM_ID)));
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
  protected void doStop() {}

  @Override
  protected Iterable<Field> getAllConfigurationFields() {
    return ScyllaConnectorConfig.ALL_FIELDS;
  }

  @Override
  public String version() {
    return Module.version();
  }
}

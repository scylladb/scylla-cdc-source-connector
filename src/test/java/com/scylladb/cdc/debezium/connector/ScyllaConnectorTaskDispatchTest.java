package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.Mockito;

/** The task's EventDispatcher wiring, on a real queue: context headers and heartbeats. */
@Isolated
class ScyllaConnectorTaskDispatchTest {

  // Loading ScyllaConnectorTask points Flogger at log4j, which other tests in this JVM lack, so
  // these tests run alone and put the property back
  private static final String FLOGGER_BACKEND = System.getProperty("flogger.backend_factory");

  @AfterAll
  static void restoreFloggerBackend() {
    if (FLOGGER_BACKEND == null) {
      System.clearProperty("flogger.backend_factory");
    } else {
      System.setProperty("flogger.backend_factory", FLOGGER_BACKEND);
    }
  }

  private static final TaskId TASK_ID =
      new TaskId(
          new GenerationId(new Timestamp(new Date(1000))),
          new VNodeId(0),
          new TableName("ks", "t"));
  private static final CollectionId COLLECTION = new CollectionId(new TableName("ks", "t"));
  private static final Schema ROW_SCHEMA =
      SchemaBuilder.struct().name("ks.t.Value").field("pk", Schema.INT32_SCHEMA).build();
  private static final Set<String> CONTEXT_HEADERS =
      Set.of(
          "__debezium.context.connectorLogicalName",
          "__debezium.context.taskId",
          "__debezium.context.connectorName",
          "__debezium.context.runId");

  private ChangeEventQueue<DataChangeEvent> queue;
  private ScyllaPartition partition;
  private TaskStateOffsetContext offset;
  private TopicNamingStrategy topicNamingStrategy;

  @SuppressWarnings("unchecked")
  private EventDispatcher<ScyllaPartition, CollectionId> dispatcher(Boolean extendedHeaders) {
    Configuration.Builder builder =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.t")
            // ScyllaConnectorConfig.taskConfig sets this default on every task
            .with("heartbeat.interval.ms", 30000);
    if (extendedHeaders != null) {
      builder.with("extended.headers.enabled", extendedHeaders);
    }
    Configuration configuration = builder.build();
    ScyllaConnectorConfig config = new ScyllaConnectorConfig(configuration);

    SourceInfo sourceInfo = new SourceInfo(config, TASK_ID);
    sourceInfo.setTaskState(
        new TaskState(
            new Timestamp(new Date(10_000)), new Timestamp(new Date(20_000)), Optional.empty()));
    ScyllaOffsetContext offsetContext =
        new ScyllaOffsetContext(Map.of(TASK_ID, sourceInfo), new TransactionContext());
    partition = new ScyllaPartition(offsetContext, sourceInfo);
    offset = offsetContext.taskStateOffsetContext(TASK_ID);
    topicNamingStrategy = DefaultTopicNamingStrategy.create(config);

    Envelope envelope =
        Envelope.defineSchema()
            .withName("ks.t.Envelope")
            .withRecord(ROW_SCHEMA)
            .withSource(SchemaBuilder.struct().optional().build())
            .build();
    DataCollectionSchema collectionSchema = Mockito.mock(DataCollectionSchema.class);
    Mockito.when(collectionSchema.id()).thenReturn(COLLECTION);
    Mockito.when(collectionSchema.keySchema()).thenReturn(ROW_SCHEMA);
    Mockito.when(collectionSchema.getEnvelopeSchema()).thenReturn(envelope);
    DatabaseSchema<CollectionId> schema = Mockito.mock(DatabaseSchema.class);
    Mockito.when(schema.schemaFor(COLLECTION)).thenReturn(collectionSchema);

    ScyllaTaskContext taskContext = new ScyllaTaskContext(configuration, List.of());
    queue =
        new ChangeEventQueue.Builder<DataChangeEvent>()
            .pollInterval(Duration.ofMillis(10))
            .maxBatchSize(10)
            .maxQueueSize(10)
            .loggingContextSupplier(() -> taskContext.configureLoggingContext("test"))
            .build();
    return ScyllaConnectorTask.createDispatcher(
        config,
        topicNamingStrategy,
        schema,
        queue,
        new ScyllaEventMetadataProvider(),
        null,
        taskContext);
  }

  private void dispatchInsert(EventDispatcher<ScyllaPartition, CollectionId> dispatcher)
      throws InterruptedException {
    Struct row = new Struct(ROW_SCHEMA).put("pk", 1);
    dispatcher.dispatchDataChangeEvent(
        partition,
        COLLECTION,
        new ChangeRecordEmitter<ScyllaPartition>() {
          @Override
          public void emitChangeRecords(
              DataCollectionSchema schema, Receiver<ScyllaPartition> receiver)
              throws InterruptedException {
            Struct value = schema.getEnvelopeSchema().create(row, null, Instant.EPOCH);
            receiver.changeRecord(
                partition,
                schema,
                Envelope.Operation.CREATE,
                row,
                value,
                offset,
                new ConnectHeaders());
          }

          @Override
          public ScyllaPartition getPartition() {
            return partition;
          }

          @Override
          public TaskStateOffsetContext getOffset() {
            return offset;
          }

          @Override
          public Envelope.Operation getOperation() {
            return Envelope.Operation.CREATE;
          }
        });
  }

  private SourceRecord pollOne() throws InterruptedException {
    List<DataChangeEvent> events = queue.poll();
    assertEquals(1, events.size());
    return events.get(0).getRecord();
  }

  private static Set<String> contextHeaders(SourceRecord record) {
    Set<String> keys = new HashSet<>();
    for (Header header : record.headers()) {
      keys.add(header.key());
    }
    return keys.stream().filter(CONTEXT_HEADERS::contains).collect(Collectors.toSet());
  }

  @Test
  void dataChangeRecord_carriesNoContextHeadersByDefault() throws InterruptedException {
    dispatchInsert(dispatcher(null));

    SourceRecord record = pollOne();
    assertEquals(topicNamingStrategy.dataChangeTopic(COLLECTION), record.topic());
    assertEquals(Set.of(), contextHeaders(record));
  }

  @Test
  void dataChangeRecord_carriesTheFourContextHeadersWhenEnabled() throws InterruptedException {
    dispatchInsert(dispatcher(true));

    assertEquals(CONTEXT_HEADERS, contextHeaders(pollOne()));
  }

  @Test
  void heartbeat_reachesTheTaskQueueWithoutContextHeaders() throws InterruptedException {
    // Debezium 3.x heartbeats enqueue themselves, so this fails unless the queue is wired in
    dispatcher(true).alwaysDispatchHeartbeatEvent(partition, offset);

    SourceRecord heartbeat = pollOne();
    assertEquals(topicNamingStrategy.heartbeatTopic(), heartbeat.topic());
    assertEquals(offset.getOffset(), heartbeat.sourceOffset());
    assertTrue(contextHeaders(heartbeat).isEmpty());
  }
}

package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

public class ScyllaChangeEventSourceFactory
    implements ChangeEventSourceFactory<ScyllaPartition, ScyllaOffsetContext> {

  private final ScyllaConnectorConfig configuration;
  private final ScyllaTaskContext taskContext;
  private final DatabaseSchema<CollectionId> schema;
  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final Clock clock;

  public ScyllaChangeEventSourceFactory(
      ScyllaConnectorConfig configuration,
      ScyllaTaskContext context,
      DatabaseSchema<CollectionId> schema,
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      Clock clock) {
    this.configuration = configuration;
    this.taskContext = context;
    this.schema = schema;
    this.dispatcher = dispatcher;
    this.clock = clock;
  }

  @Override
  public SnapshotChangeEventSource<ScyllaPartition, ScyllaOffsetContext>
      getSnapshotChangeEventSource(
          SnapshotProgressListener<ScyllaPartition> snapshotProgressListener,
          NotificationService<ScyllaPartition, ScyllaOffsetContext> notificationService) {
    return new ScyllaSnapshotChangeEventSource(
        configuration, snapshotProgressListener, notificationService);
  }

  @Override
  public StreamingChangeEventSource<ScyllaPartition, ScyllaOffsetContext>
      getStreamingChangeEventSource() {
    return new ScyllaStreamingChangeEventSource(
        configuration, taskContext, schema, dispatcher, clock);
  }
}

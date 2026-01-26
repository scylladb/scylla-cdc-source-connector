package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.schema.DataCollectionSchema;
import java.util.Optional;

public class ScyllaInconsistentSchemaHandler
    implements EventDispatcher.InconsistentSchemaHandler<ScyllaPartition, CollectionId> {
  @Override
  public Optional<DataCollectionSchema> handle(
      ScyllaPartition partition,
      CollectionId collectionId,
      ChangeRecordEmitter changeRecordEmitter) {
    ScyllaChangeRecordEmitter scyllaChangeRecordEmitter =
        (ScyllaChangeRecordEmitter) changeRecordEmitter;
    ScyllaSchema scyllaSchema = scyllaChangeRecordEmitter.getSchema();
    ChangeSchema changeSchema = scyllaChangeRecordEmitter.getChangeSchema();
    if (changeSchema == null) {
      return Optional.empty();
    }
    ScyllaCollectionSchema scyllaCollectionSchema =
        scyllaSchema.updateChangeSchema(collectionId, changeSchema);

    if (scyllaCollectionSchema == null) {
      return Optional.empty();
    }

    return Optional.of(scyllaCollectionSchema);
  }
}

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

    // Handle legacy emitter type
    if (changeRecordEmitter instanceof ScyllaChangeRecordEmitterLegacy) {
      ScyllaChangeRecordEmitterLegacy legacyEmitter =
          (ScyllaChangeRecordEmitterLegacy) changeRecordEmitter;
      ScyllaSchemaLegacy scyllaSchema = legacyEmitter.getSchema();
      ChangeSchema changeSchema = legacyEmitter.getChange().getSchema();
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

    // Handle advanced emitter type
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

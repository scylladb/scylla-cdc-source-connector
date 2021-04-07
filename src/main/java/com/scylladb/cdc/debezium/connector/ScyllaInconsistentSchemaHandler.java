package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.schema.DataCollectionSchema;

import java.util.Optional;

public class ScyllaInconsistentSchemaHandler implements EventDispatcher.InconsistentSchemaHandler<CollectionId> {
    @Override
    public Optional<DataCollectionSchema> handle(CollectionId collectionId, ChangeRecordEmitter changeRecordEmitter) {
        ScyllaChangeRecordEmitter scyllaChangeRecordEmitter = (ScyllaChangeRecordEmitter) changeRecordEmitter;
        RawChange change = scyllaChangeRecordEmitter.getChange();
        ScyllaSchema scyllaSchema = scyllaChangeRecordEmitter.getSchema();
        ScyllaCollectionSchema scyllaCollectionSchema = scyllaSchema.updateChangeSchema(collectionId, change.getSchema());

        if (scyllaCollectionSchema == null) {
            return Optional.empty();
        }

        return Optional.of(scyllaCollectionSchema);
    }
}

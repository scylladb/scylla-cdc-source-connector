package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TableName;
import io.debezium.schema.DataCollectionId;

import java.util.Objects;

public class CollectionId implements DataCollectionId {

    private final TableName tableName;

    public CollectionId(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public String identifier() {
        return tableName.keyspace + "." + tableName.name;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CollectionId that = (CollectionId) o;
        return tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }
}

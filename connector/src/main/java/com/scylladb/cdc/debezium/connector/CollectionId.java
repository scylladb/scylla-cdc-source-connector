package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TableName;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;
import java.util.Collections;
import java.util.List;
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

  @Override
  public List<String> parts() {
    return Collect.arrayListOf(tableName.keyspace, tableName.name);
  }

  @Override
  public List<String> databaseParts() {
    return Collect.arrayListOf(tableName.keyspace, tableName.name);
  }

  @Override
  public List<String> schemaParts() {
    return Collections.emptyList();
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

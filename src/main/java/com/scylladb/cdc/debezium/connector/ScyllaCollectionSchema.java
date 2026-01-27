package com.scylladb.cdc.debezium.connector;

import io.debezium.data.Envelope;
import io.debezium.schema.DataCollectionSchema;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;

public class ScyllaCollectionSchema implements DataCollectionSchema {
  private final CollectionId id;
  private final Schema keySchema;
  private final Schema valueSchema;
  private final Schema beforeSchema;
  private final Schema afterSchema;
  private final Schema diffSchema;
  private final Map<String, Schema> cellSchemas;
  private final Map<String, Schema> diffCellSchemas;
  private final Envelope envelopeSchema;

  public ScyllaCollectionSchema(
      CollectionId id,
      Schema keySchema,
      Schema valueSchema,
      Schema beforeSchema,
      Schema afterSchema,
      Schema diffSchema,
      Map<String, Schema> cellSchemas,
      Map<String, Schema> diffCellSchemas,
      Envelope envelopeSchema) {
    this.id = id;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.beforeSchema = beforeSchema;
    this.afterSchema = afterSchema;
    this.diffSchema = diffSchema;
    this.cellSchemas = cellSchemas;
    this.diffCellSchemas = diffCellSchemas;
    this.envelopeSchema = envelopeSchema;
  }

  @Override
  public CollectionId id() {
    return id;
  }

  @Override
  public Schema keySchema() {
    return keySchema;
  }

  public Schema beforeSchema() {
    return beforeSchema;
  }

  public Schema afterSchema() {
    return afterSchema;
  }

  public Schema diffSchema() {
    return diffSchema;
  }

  public Schema valueSchema() {
    return valueSchema;
  }

  public Schema cellSchema(String columnName) {
    return cellSchemas.get(columnName);
  }

  public Schema diffCellSchema(String columnName) {
    return diffCellSchemas != null ? diffCellSchemas.get(columnName) : null;
  }

  @Override
  public Envelope getEnvelopeSchema() {
    return envelopeSchema;
  }
}

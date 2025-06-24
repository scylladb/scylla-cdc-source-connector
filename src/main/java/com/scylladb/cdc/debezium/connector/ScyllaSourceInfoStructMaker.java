package com.scylladb.cdc.debezium.connector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ScyllaSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {
  private Schema schema;

  @Override
  public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
    super.init(connector, version, connectorConfig);
    schema =
        commonSchemaBuilder()
            .name("com.scylladb.cdc.debezium.connector")
            .field(SourceInfo.KEYSPACE_NAME, Schema.STRING_SCHEMA)
            .field(SourceInfo.TABLE_NAME, Schema.STRING_SCHEMA)
            .build();
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public Struct struct(SourceInfo sourceInfo) {
    return super.commonStruct(sourceInfo)
        .put(SourceInfo.KEYSPACE_NAME, sourceInfo.getTableName().keyspace)
        .put(SourceInfo.TABLE_NAME, sourceInfo.getTableName().name)
        .put(AbstractSourceInfo.TIMESTAMP_US_KEY, sourceInfo.timestampUs());
  }
}

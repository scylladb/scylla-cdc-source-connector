package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;

public class ScyllaEventMetadataProvider implements EventMetadataProvider {
  @Override
  public Instant getEventTimestamp(
      DataCollectionId dataCollectionId, OffsetContext offsetContext, Object o, Struct struct) {
    return null;
  }

  @Override
  public Map<String, String> getEventSourcePosition(
      DataCollectionId dataCollectionId, OffsetContext offsetContext, Object o, Struct struct) {
    return null;
  }

  @Override
  public String getTransactionId(
      DataCollectionId dataCollectionId, OffsetContext offsetContext, Object o, Struct struct) {
    return null;
  }
}

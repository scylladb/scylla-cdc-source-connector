package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.ExtractNewRecordState;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Transforms Scylla CDC records by extracting the new record state from the Debezium envelope.
 *
 * <p>This transform extends Debezium's ExtractNewRecordState to extract the "after" record from the
 * envelope structure, producing a flat record with just the column values.
 *
 * <p>For non-frozen collections (SET, LIST, MAP, UDT), the values contain delta information:
 *
 * <ul>
 *   <li>SET/LIST: Array of elements (added and deleted elements in delta)
 *   <li>MAP: Array of {key, value} structs (null value indicates deletion)
 *   <li>UDT: Struct with field values (only modified fields present)
 * </ul>
 */
public class ScyllaExtractNewRecordState<R extends ConnectRecord<R>>
    extends ExtractNewRecordState<R> {}

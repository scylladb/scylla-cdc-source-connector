package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaChangeRecordEmitter
    extends AbstractChangeRecordEmitter<ScyllaPartition, ScyllaCollectionSchema> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaChangeRecordEmitter.class);

  /**
   * Error marker prefix used in log messages for connector errors during record emission. This
   * marker can be searched for in logs to identify connector-level errors.
   */
  public static final String CONNECTOR_ERROR_MARKER = "[SCYLLA-CDC-CONNECTOR-ERROR]";

  /** CDC column prefix for deleted column indicators. */
  private static final String CDC_DELETED_PREFIX = "cdc$deleted_";

  private static final String CDC_DELETED_ELEMENTS_PREFIX = "cdc$deleted_elements_";

  private static final String CDC_PREFIX = "cdc$";

  /**
   * Safely retrieves a cell from a RawChange, returning null if an exception occurs.
   *
   * @param change the RawChange to get the cell from
   * @param columnName the name of the column
   * @return the Cell, or null if an exception occurs or the cell doesn't exist
   */
  private static Cell getCellSafe(RawChange change, String columnName) {
    try {
      return change.getCell(columnName);
    } catch (Exception e) {
      return null;
    }
  }

  private final ScyllaSchema schema;
  private final TaskInfo taskInfo;
  private final ScyllaConnectorConfig connectorConfig;

  public ScyllaChangeRecordEmitter(
      ScyllaPartition partition,
      TaskInfo taskInfo,
      OffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig) {
    super(partition, offsetContext, clock, connectorConfig);
    this.taskInfo = taskInfo;
    this.schema = schema;
    this.connectorConfig = connectorConfig;
  }

  public ChangeSchema getChangeSchema() {
    RawChange change = taskInfo.getAnyImage();
    if (change == null) {
      return null;
    }
    return change.getSchema();
  }

  public ScyllaSchema getSchema() {
    return schema;
  }

  @Override
  public Envelope.Operation getOperation() {
    RawChange.OperationType operationType = taskInfo.getChange().getOperationType();
    switch (operationType) {
      case ROW_UPDATE:
        return Envelope.Operation.UPDATE;
      case ROW_INSERT:
        return Envelope.Operation.CREATE;
      case PARTITION_DELETE: // See comment in ScyllaChangesConsumer on the support of partition
      // deletes.
      case ROW_DELETE:
        return Envelope.Operation.DELETE;
      default:
        String errorMsg = String.format("Unsupported operation type: %s.", operationType);
        LOGGER.error("{} {}", CONNECTOR_ERROR_MARKER, errorMsg);
        throw new RuntimeException(errorMsg);
    }
  }

  @Override
  protected void emitReadRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Builds the keyStruct with primary key values from the best available source.
   *
   * <p>Prefers postImage over preImage, falling back to the change record if neither is available.
   *
   * @param keySchema the schema for the key struct
   * @param anyImage the image to extract key values from
   * @return the populated key struct
   */
  private Struct createKeyStruct(
      Schema keySchema, RawChange anyImage, ScyllaCollectionSchema collectionSchema) {
    if (!includePkInKafkaKey()) {
      return null;
    }
    Struct keyStruct = new Struct(keySchema);
    fillKeyStructFromImage(keyStruct, anyImage, collectionSchema);
    return keyStruct;
  }

  private ScyllaCollectionSchema getOrCreateSchema(
      ScyllaCollectionSchema scyllaCollectionSchema, RawChange anyImage) {
    if (anyImage == null) {
      return scyllaCollectionSchema;
    }
    return this.schema.updateChangeSchema(scyllaCollectionSchema.id(), anyImage.getSchema());
  }

  @Override
  protected void emitCreateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    RawChange anyImage = taskInfo.getAnyImage();
    scyllaCollectionSchema = getOrCreateSchema(scyllaCollectionSchema, anyImage);

    // Build Kafka record key based on configuration
    Struct keyStruct =
        createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage, scyllaCollectionSchema);

    // Build after struct with PK based on configuration
    Struct afterStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.afterSchema(),
            taskInfo.getPostImage(),
            includePkInPayloadAfter(),
            scyllaCollectionSchema);

    // Build before struct with PK based on configuration
    Struct beforeStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.beforeSchema(),
            taskInfo.getPreImage(),
            includePkInPayloadBefore(),
            scyllaCollectionSchema);

    Struct payloadKeyStruct =
        fillPayloadKeyStruct(scyllaCollectionSchema.keySchema(), anyImage, scyllaCollectionSchema);

    Struct envelope =
        generalizedEnvelope(
            scyllaCollectionSchema.getEnvelopeSchema().schema(),
            beforeStruct,
            afterStruct,
            payloadKeyStruct,
            null,
            getOffset().getSourceInfo(),
            getClock().currentTimeAsInstant(),
            Envelope.Operation.CREATE);

    // Build Kafka headers if configured
    ConnectHeaders headers = buildPkHeaders(anyImage, scyllaCollectionSchema);

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        headers);
  }

  @Override
  protected void emitUpdateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    RawChange anyImage = taskInfo.getAnyImage();
    scyllaCollectionSchema = getOrCreateSchema(scyllaCollectionSchema, anyImage);

    // Build Kafka record key based on configuration
    Struct keyStruct =
        createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage, scyllaCollectionSchema);

    ScyllaConnectorConfig.CdcIncludeMode beforeMode = connectorConfig.getCdcIncludeBefore();
    ScyllaConnectorConfig.CdcIncludeMode afterMode = connectorConfig.getCdcIncludeAfter();

    Struct afterStruct;
    Struct beforeStruct;
    Set<String> modifiedColumns = getModifiedColumns(taskInfo.getChange());

    if (beforeMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED
        || afterMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED) {

      if (afterMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED) {
        afterStruct =
            fillStructWithOnlyUpdatedColumns(
                scyllaCollectionSchema.afterSchema(),
                taskInfo.getPostImage(),
                modifiedColumns,
                includePkInPayloadAfter(),
                scyllaCollectionSchema);
      } else {
        afterStruct =
            fillStructWithFullImage(
                scyllaCollectionSchema.afterSchema(),
                taskInfo.getPostImage(),
                includePkInPayloadAfter(),
                scyllaCollectionSchema);
      }

      if (beforeMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED) {
        beforeStruct =
            fillStructWithOnlyUpdatedColumns(
                scyllaCollectionSchema.beforeSchema(),
                taskInfo.getPreImage(),
                modifiedColumns,
                includePkInPayloadBefore(),
                scyllaCollectionSchema);
      } else {
        beforeStruct =
            fillBeforeStructForUpdate(
                scyllaCollectionSchema,
                taskInfo.getPreImage(),
                taskInfo.getPostImage(),
                includePkInPayloadBefore(),
                modifiedColumns);
      }
    } else {
      // Original behavior for FULL/NONE modes
      afterStruct =
          fillStructWithFullImage(
              scyllaCollectionSchema.afterSchema(),
              taskInfo.getPostImage(),
              includePkInPayloadAfter(),
              scyllaCollectionSchema);

      beforeStruct =
          fillBeforeStructForUpdate(
              scyllaCollectionSchema,
              taskInfo.getPreImage(),
              taskInfo.getPostImage(),
              includePkInPayloadBefore(),
              modifiedColumns);
    }

    Struct payloadKeyStruct =
        fillPayloadKeyStruct(scyllaCollectionSchema.keySchema(), anyImage, scyllaCollectionSchema);

    Struct envelope =
        generalizedEnvelope(
            scyllaCollectionSchema.getEnvelopeSchema().schema(),
            beforeStruct,
            afterStruct,
            payloadKeyStruct,
            null,
            getOffset().getSourceInfo(),
            getClock().currentTimeAsInstant(),
            Envelope.Operation.UPDATE);

    // Build Kafka headers if configured
    ConnectHeaders headers = buildPkHeaders(anyImage, scyllaCollectionSchema);

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        headers);
  }

  @Override
  protected void emitDeleteRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    RawChange preImage = taskInfo.getPreImage();
    RawChange anyImage = taskInfo.getAnyImage();

    scyllaCollectionSchema = getOrCreateSchema(scyllaCollectionSchema, anyImage);
    Struct keyStruct =
        createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage, scyllaCollectionSchema);

    // Build before struct with PK based on configuration
    Struct beforeStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.beforeSchema(),
            preImage,
            includePkInPayloadBefore(),
            scyllaCollectionSchema);

    Struct payloadKeyStruct =
        fillPayloadKeyStruct(scyllaCollectionSchema.keySchema(), anyImage, scyllaCollectionSchema);

    Struct envelope =
        generalizedEnvelope(
            scyllaCollectionSchema.getEnvelopeSchema().schema(),
            beforeStruct,
            null,
            payloadKeyStruct,
            null,
            getOffset().getSourceInfo(),
            getClock().currentTimeAsInstant(),
            Envelope.Operation.DELETE);

    // Build Kafka headers if configured (use same fallback as payloadKeyStruct)
    ConnectHeaders headers = buildPkHeaders(anyImage, scyllaCollectionSchema);

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        headers);
  }

  /**
   * Gets the set of column names that were modified in an update operation.
   *
   * <p>Scylla CDC change records contain information about which columns were modified:
   *
   * <ul>
   *   <li>Columns with non-null values in the change record were set to a new value
   *   <li>Columns with {@code cdc$deleted_<column>} = true were explicitly set to NULL
   * </ul>
   *
   * <p>This method combines both indicators to correctly identify all modified columns, including
   * NULL to value transitions.
   *
   * @param change the change record containing column values and cdc$deleted_* indicators
   * @return set of modified column names, or empty set if change is null
   */
  private Set<String> getModifiedColumns(RawChange change) {
    if (change == null) {
      return Collections.emptySet();
    }

    Set<String> modifiedColumns = new HashSet<>();
    for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
      if (isPrimaryKeyColumn(cdef)) {
        continue;
      }
      String columnName = cdef.getColumnName();

      if (columnName.startsWith(CDC_PREFIX)) {
        continue;
      }
      // Check if column has a new value (was set to a non-null value)
      Cell valueCell = getCellSafe(change, columnName);
      if (valueCell != null && valueCell.getAsObject() != null) {
        modifiedColumns.add(columnName);
        continue;
      }

      // Check if column was explicitly deleted (set to NULL)
      Cell deletedCell = getCellSafe(change, CDC_DELETED_PREFIX + columnName);
      if (deletedCell != null && Boolean.TRUE.equals(deletedCell.getBoolean())) {
        modifiedColumns.add(columnName);
        continue;
      }

      // Check if collection has deleted elements (for non-frozen collections)
      // cdc$deleted_elements_<col> only exists for non-frozen LIST, SET, and MAP columns
      Cell deletedElementsCell = getCellSafe(change, CDC_DELETED_ELEMENTS_PREFIX + columnName);
      if (deletedElementsCell != null && deletedElementsCell.getAsObject() != null) {
        modifiedColumns.add(columnName);
      }
    }
    return modifiedColumns;
  }

  /**
   * Fills only the key struct with primary key values from an image.
   *
   * <p>Iterates over the key schema fields in order (which should be partition keys first, then
   * clustering keys) to ensure proper ordering of the primary key columns.
   *
   * @param keyStruct the key struct to fill
   * @param image the preimage or postimage to get primary key values from
   */
  private void fillKeyStructFromImage(
      Struct keyStruct, RawChange image, ScyllaCollectionSchema collectionSchema) {
    // Iterate over key schema fields in order to maintain proper PK column ordering
    for (Field field : keyStruct.schema().fields()) {
      String columnName = field.name();
      Cell cell = getCellSafe(image, columnName);
      if (cell != null) {
        Object value = translateCellToKafka(cell, collectionSchema.cellSchema(columnName));
        keyStruct.put(columnName, value);
      }
    }
  }

  /**
   * Builds and fills a value struct with only the modified columns from an image.
   *
   * <p>This method is used for the 'only-updated' mode, where only columns that were modified by
   * the operation are included in the before/after structs. Primary key columns are included based
   * on the includePk parameter.
   *
   * @param structSchema the schema to use for creating the struct (beforeSchema or afterSchema)
   * @param image the image (preimage or postimage) containing the data
   * @param modifiedColumns the set of column names that were modified
   * @param includePk whether to include primary key columns
   * @return the populated struct with only modified columns, or null if image is null
   */
  private Struct fillStructWithOnlyUpdatedColumns(
      Schema structSchema,
      RawChange image,
      Set<String> modifiedColumns,
      boolean includePk,
      ScyllaCollectionSchema collectionSchema) {
    if (image == null) {
      return null;
    }
    Struct valueStruct = new Struct(structSchema);
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      String columnName = cdef.getColumnName();
      Object value =
          translateCellToKafka(
              getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));

      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          valueStruct.put(columnName, value);
        }
      } else if (modifiedColumns.contains(columnName) && value != null) {
        valueStruct.put(columnName, value);
      }
    }
    return valueStruct;
  }

  /**
   * Builds and fills a value struct with data from a full image (preimage or postimage). This
   * method doesn't check cdc$deleted_ columns because preimage/postimage contain complete row data
   * with all column values.
   *
   * @param structSchema the schema to use for creating the struct (beforeSchema or afterSchema)
   * @param image the preimage or postimage containing the full row data, or null
   * @param includePk whether to include primary key columns
   * @param collectionSchema the collection schema for looking up cell schemas
   * @return the populated struct, or null if image is null
   */
  private Struct fillStructWithFullImage(
      Schema structSchema,
      RawChange image,
      boolean includePk,
      ScyllaCollectionSchema collectionSchema) {
    if (image == null) {
      return null;
    }
    Struct valueStruct = new Struct(structSchema);
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      String columnName = cdef.getColumnName();
      Object value =
          translateCellToKafka(
              getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));

      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          valueStruct.put(columnName, value);
        }
      } else if (value != null) {
        valueStruct.put(columnName, value);
      }
    }
    return valueStruct;
  }

  /**
   * Builds the "before" struct for UPDATE operations by combining data from preimage and postimage.
   *
   * <p>Scylla CDC preimage only contains OLD values for columns that were MODIFIED. For unchanged
   * columns, their old value equals their new value, so we get them from the postimage.
   *
   * <p>This method handles three cases:
   *
   * <ul>
   *   <li>Both preimage and postimage present: combines data from both sources
   *   <li>Only preimage present: uses preimage as full image
   *   <li>Neither present: returns null
   * </ul>
   *
   * <p>When both images are present:
   *
   * <ul>
   *   <li>For columns present in preimage (modified columns): uses preimage values
   *   <li>For columns NOT in preimage but in postimage (unchanged columns): uses postimage values
   *   <li>PARTITION_KEY and CLUSTERING_KEY columns are included from postimage (if includePk is
   *       true)
   * </ul>
   *
   * @param collectionSchema the collection schema
   * @param preImage the preimage containing old values for modified columns, or null
   * @param postImage the postimage containing all column values after the change, or null
   * @param includePk whether to include primary key columns
   * @param modifiedColumns the set of modified column names (pre-computed from preImage)
   * @return the populated before struct, or null if no preimage is available
   */
  private Struct fillBeforeStructForUpdate(
      ScyllaCollectionSchema collectionSchema,
      RawChange preImage,
      RawChange postImage,
      boolean includePk,
      Set<String> modifiedColumns) {
    // If no preimage, we have no "before" data
    if (preImage == null) {
      return null;
    }

    // If no postimage, treat preimage as a full image
    if (postImage == null) {
      return fillStructWithFullImage(
          collectionSchema.beforeSchema(), preImage, includePk, collectionSchema);
    }

    // Both images present - combine data from preimage and postimage
    Struct valueStruct = new Struct(collectionSchema.beforeSchema());

    // Fill from postImage - this gives us unchanged columns and primary keys
    for (ChangeSchema.ColumnDefinition cdef : postImage.getSchema().getNonCdcColumnDefinitions()) {
      String columnName = cdef.getColumnName();

      if (isPrimaryKeyColumn(cdef)) {
        if (!includePk) {
          continue;
        }
      } else if (modifiedColumns.contains(columnName)) {
        continue;
      }
      Object value =
          translateCellToKafka(
              getCellSafe(postImage, columnName), collectionSchema.cellSchema(columnName));
      valueStruct.put(columnName, value);
    }

    // Fill modified columns from preImage (which has the OLD values)
    for (String columnName : modifiedColumns) {
      Object value =
          translateCellToKafka(
              getCellSafe(preImage, columnName), collectionSchema.cellSchema(columnName));
      valueStruct.put(columnName, value);
    }

    return valueStruct;
  }

  private Struct generalizedEnvelope(
      Schema schema,
      Object before,
      Object after,
      Object payloadKey,
      Object diff,
      Struct source,
      Instant timestamp,
      Envelope.Operation operationType) {
    Struct struct = new Struct(schema);
    struct.put(Envelope.FieldName.OPERATION, operationType.code());
    if (before != null) {
      struct.put(Envelope.FieldName.BEFORE, before);
    }
    if (after != null) {
      struct.put(Envelope.FieldName.AFTER, after);
    }
    // Add optional key field if schema contains it and value is not null
    String keyFieldName = this.schema.getPayloadKeyFieldName();
    if (payloadKey != null && schema.field(keyFieldName) != null) {
      struct.put(keyFieldName, payloadKey);
    }
    // Add optional diff field if schema contains it and value is not null
    if (diff != null && schema.field(ScyllaSchema.FIELD_DIFF) != null) {
      struct.put(ScyllaSchema.FIELD_DIFF, diff);
    }
    if (source != null) {
      struct.put(Envelope.FieldName.SOURCE, source);
    }
    if (timestamp != null) {
      struct.put(Envelope.FieldName.TIMESTAMP, timestamp.toEpochMilli());
    }
    return struct;
  }

  /**
   * Translates a Cell to Kafka representation for scalar and frozen collection types.
   *
   * @param cell the cell containing the value to translate
   * @param cellSchema the pre-computed Kafka Connect schema for this cell (from
   *     ScyllaCollectionSchema.cellSchema)
   * @return the Kafka-compatible representation of the cell value, or null if the cell is null
   */
  private Object translateCellToKafka(Cell cell, Schema cellSchema) {
    if (cell == null || cell.getAsObject() == null) {
      return null;
    }
    return translateFieldToKafka(cell, cellSchema);
  }

  /** Converts a Scylla CDC field value into a Kafka Connect-compatible representation. */
  private Object translateFieldToKafka(
      com.scylladb.cdc.model.worker.cql.Field field, Schema resultSchema) {
    ChangeSchema.DataType dataType = field.getDataType();

    if (field.getAsObject() == null) {
      return null;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DECIMAL) {
      return field.getDecimal().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UUID) {
      return field.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.TIMEUUID) {
      return field.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.VARINT) {
      return field.getVarint().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.INET) {
      return field.getInet().getHostAddress();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DATE) {
      CqlDate cqlDate = field.getDate();
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.clear();
      // Months start from 0 in Calendar:
      calendar.set(cqlDate.getYear(), cqlDate.getMonth() - 1, cqlDate.getDay());
      return Date.from(calendar.toInstant());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DURATION) {
      return field.getDuration().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.LIST) {
      Schema innerSchema = resultSchema != null ? resultSchema.valueSchema() : null;
      return field.getList().stream()
          .map((element) -> translateFieldToKafka(element, innerSchema))
          .collect(Collectors.toList());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.SET) {
      Schema innerSchema = resultSchema != null ? resultSchema.valueSchema() : null;
      return field.getSet().stream()
          .map((element) -> translateFieldToKafka(element, innerSchema))
          .collect(Collectors.toList());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.MAP) {
      Map<com.scylladb.cdc.model.worker.cql.Field, com.scylladb.cdc.model.worker.cql.Field> map =
          field.getMap();

      // Check if this is a non-frozen list (stored as map<timeuuid, V>)
      // Non-frozen lists have TIMEUUID keys AND the schema is ARRAY of non-struct values.
      // Frozen map<timeuuid, V> has the same CDC structure but schema is ARRAY of {key, value}.
      List<ChangeSchema.DataType> typeArgs = dataType.getTypeArguments();
      boolean isMapEntrySchema =
          resultSchema != null
              && resultSchema.type() == Schema.Type.ARRAY
              && resultSchema.valueSchema() != null
              && resultSchema.valueSchema().type() == Schema.Type.STRUCT
              && resultSchema.valueSchema().field("key") != null
              && resultSchema.valueSchema().field("value") != null;

      if (typeArgs != null
          && typeArgs.size() == 2
          && typeArgs.get(0).getCqlType() == ChangeSchema.CqlType.TIMEUUID
          && resultSchema != null
          && resultSchema.type() == Schema.Type.ARRAY
          && !isMapEntrySchema) {
        // This is a non-frozen list - extract values only, sorted by key (timeuuid preserves order)
        Schema valueSchema = resultSchema.valueSchema();
        return map.entrySet().stream()
            .sorted(
                (e1, e2) -> {
                  // Sort by timeuuid key to preserve insertion order
                  UUID k1 = e1.getKey().getUUID();
                  UUID k2 = e2.getKey().getUUID();
                  return k1.compareTo(k2);
                })
            .map(e -> translateFieldToKafka(e.getValue(), valueSchema))
            .collect(Collectors.toList());
      }

      if (resultSchema != null && resultSchema.type() == Schema.Type.ARRAY) {
        List<Object> entries = new ArrayList<>();
        Schema entrySchema = resultSchema.valueSchema();
        if (entrySchema == null) {
          String errorMsg = "Array schema for MAP must have non-null value schema";
          LOGGER.error("{} {}", CONNECTOR_ERROR_MARKER, errorMsg);
          throw new IllegalStateException(errorMsg);
        }
        Schema keySchema = entrySchema.field("key").schema();
        Schema valueSchema = entrySchema.field("value").schema();
        map.forEach(
            (key, value) -> {
              Object kafkaKey = translateFieldToKafka(key, keySchema);
              Object kafkaValue = translateFieldToKafka(value, valueSchema);
              entries.add(createListElementStruct(entrySchema, kafkaKey, kafkaValue));
            });
        return entries;
      }
      Map<Object, Object> kafkaMap = new LinkedHashMap<>();
      Schema keySchema = resultSchema != null ? resultSchema.keySchema() : null;
      Schema valueSchema = resultSchema != null ? resultSchema.valueSchema() : null;
      map.forEach(
          (key, value) -> {
            Object kafkaKey = translateFieldToKafka(key, keySchema);
            Object kafkaValue = translateFieldToKafka(value, valueSchema);
            kafkaMap.put(kafkaKey, kafkaValue);
          });
      return kafkaMap;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.TUPLE) {
      if (resultSchema == null) {
        String errorMsg = "TUPLE type requires a non-null schema";
        LOGGER.error("{} {}", CONNECTOR_ERROR_MARKER, errorMsg);
        throw new IllegalStateException(errorMsg);
      }
      List<Field> fieldSchemas = resultSchema.fields();
      Struct tupleStruct = new Struct(resultSchema);
      List<com.scylladb.cdc.model.worker.cql.Field> tuple = field.getTuple();
      for (int i = 0; i < tuple.size(); i++) {
        Schema fieldSchema = fieldSchemas != null ? fieldSchemas.get(i).schema() : null;
        // Use "field_N" naming for Avro compatibility (Avro field names cannot start with digits)
        tupleStruct.put("field_" + i, translateFieldToKafka(tuple.get(i), fieldSchema));
      }
      return tupleStruct;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UDT) {
      if (resultSchema == null) {
        String errorMsg = "UDT type requires a non-null schema";
        LOGGER.error("{} {}", CONNECTOR_ERROR_MARKER, errorMsg);
        throw new IllegalStateException(errorMsg);
      }
      Struct udtStruct = new Struct(resultSchema);
      Map<String, com.scylladb.cdc.model.worker.cql.Field> udt = field.getUDT();
      boolean allNullOrEmpty = true;
      for (Map.Entry<String, com.scylladb.cdc.model.worker.cql.Field> entry : udt.entrySet()) {
        String name = entry.getKey();
        com.scylladb.cdc.model.worker.cql.Field value = entry.getValue();
        Schema fieldSchema =
            resultSchema.field(name) != null ? resultSchema.field(name).schema() : null;
        Object translated = translateFieldToKafka(value, fieldSchema);
        udtStruct.put(name, translated);
        // Check if field has meaningful content (not null and not empty collection)
        if (translated != null && !isEmptyCollection(translated)) {
          allNullOrEmpty = false;
        }
      }
      // Return null for empty UDTs (all fields null or empty) for consistency with frozen UDTs
      if (allNullOrEmpty) {
        return null;
      }
      return udtStruct;
    }

    return field.getAsObject();
  }

  /** Creates a list-entry struct for map-like collection elements. */
  private Struct createListElementStruct(Schema entrySchema, Object key, Object value) {
    Struct elementStruct = new Struct(entrySchema);
    elementStruct.put("key", key);
    elementStruct.put("value", value);
    return elementStruct;
  }

  /** Checks if a value is an empty collection (List, Set, or Map). */
  private static boolean isEmptyCollection(Object value) {
    if (value instanceof Collection) {
      return ((Collection<?>) value).isEmpty();
    }
    if (value instanceof Map) {
      return ((Map<?, ?>) value).isEmpty();
    }
    return false;
  }

  // ========== PK Location Configuration Helpers ==========

  /** Checks if the column is a primary key column (partition key or clustering key). */
  private static boolean isPrimaryKeyColumn(ChangeSchema.ColumnDefinition cdef) {
    ChangeSchema.ColumnType colType = cdef.getBaseTableColumnType();
    return colType == ChangeSchema.ColumnType.PARTITION_KEY
        || colType == ChangeSchema.ColumnType.CLUSTERING_KEY;
  }

  /** Checks if PK/CK should be included in the Kafka record key. */
  private boolean includePkInKafkaKey() {
    return connectorConfig.getCdcIncludePk().inKafkaKey;
  }

  /** Checks if PK/CK should be included in the 'after' struct. */
  private boolean includePkInPayloadAfter() {
    return connectorConfig.getCdcIncludePk().inPayloadAfter;
  }

  /** Checks if PK/CK should be included in the 'before' struct. */
  private boolean includePkInPayloadBefore() {
    return connectorConfig.getCdcIncludePk().inPayloadBefore;
  }

  /** Checks if PK/CK should be included in the payload 'key' field. */
  private boolean includePkInPayloadKey() {
    return connectorConfig.getCdcIncludePk().inPayloadKey;
  }

  /** Checks if PK/CK should be included in Kafka headers. */
  private boolean includePkInKafkaHeaders() {
    return connectorConfig.getCdcIncludePk().inKafkaHeaders;
  }

  /**
   * Builds Kafka headers containing PK/CK values.
   *
   * @param image the image to extract PK/CK values from
   * @param collectionSchema the collection schema for looking up cell schemas
   * @return ConnectHeaders containing pk.* and ck.* headers, or null if not configured
   */
  private ConnectHeaders buildPkHeaders(RawChange image, ScyllaCollectionSchema collectionSchema) {
    if (!includePkInKafkaHeaders() || image == null) {
      return null;
    }
    ConnectHeaders headers = new ConnectHeaders();
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      String columnName = cdef.getColumnName();
      Object value =
          translateCellToKafka(
              getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
      if (value == null) continue;

      ChangeSchema.ColumnType columnType = cdef.getBaseTableColumnType();
      String headerPrefix;
      if (columnType == ChangeSchema.ColumnType.PARTITION_KEY) {
        headerPrefix = "pk.";
      } else if (columnType == ChangeSchema.ColumnType.CLUSTERING_KEY) {
        headerPrefix = "ck.";
      } else {
        continue; // Skip non-key columns
      }

      // Convert value to string for header
      headers.addString(headerPrefix + columnName, String.valueOf(value));
    }
    return headers;
  }

  /**
   * Fills the payload 'key' struct with PK/CK values.
   *
   * @param keySchema the schema for the key struct
   * @param image the image to extract PK/CK values from
   * @param collectionSchema the collection schema for looking up cell schemas
   * @return the populated key struct, or null if not configured or image is null
   */
  private Struct fillPayloadKeyStruct(
      Schema keySchema, RawChange image, ScyllaCollectionSchema collectionSchema) {
    if (!includePkInPayloadKey() || image == null || keySchema == null) {
      return null;
    }
    Struct keyStruct = new Struct(keySchema);
    fillKeyStructFromImage(keyStruct, image, collectionSchema);
    return keyStruct;
  }
}

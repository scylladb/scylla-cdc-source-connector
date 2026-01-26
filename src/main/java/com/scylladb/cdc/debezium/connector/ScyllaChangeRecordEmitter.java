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
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaChangeRecordEmitter
    extends AbstractChangeRecordEmitter<ScyllaPartition, ScyllaCollectionSchema> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaChangeRecordEmitter.class);

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
        throw new RuntimeException(String.format("Unsupported operation type: %s.", operationType));
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
  private Struct createKeyStruct(Schema keySchema, RawChange anyImage) {
    if (!includePkInKafkaKey()) {
      return null;
    }
    Struct keyStruct = new Struct(keySchema);
    fillKeyStructFromImage(keyStruct, anyImage);
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
    Struct keyStruct = createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

    // Build after struct with PK based on configuration
    Struct afterStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.afterSchema(),
            taskInfo.getPostImage(),
            includePkInPayloadAfter());

    // Build before struct with PK based on configuration
    Struct beforeStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.beforeSchema(),
            taskInfo.getPreImage(),
            includePkInPayloadBefore());

    Struct payloadKeyStruct = fillPayloadKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

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
    ConnectHeaders headers = buildPkHeaders(anyImage);

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
    Struct keyStruct = createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

    ScyllaConnectorConfig.CdcIncludeMode beforeMode = connectorConfig.getCdcIncludeBefore();
    ScyllaConnectorConfig.CdcIncludeMode afterMode = connectorConfig.getCdcIncludeAfter();

    Struct afterStruct;
    Struct beforeStruct;
    java.util.Set<String> modifiedColumns = getModifiedColumns(taskInfo.getPreImage());

    if (beforeMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED
        || afterMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED) {

      if (afterMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED) {
        afterStruct =
            fillStructWithOnlyUpdatedColumns(
                scyllaCollectionSchema.afterSchema(),
                taskInfo.getPostImage(),
                modifiedColumns,
                includePkInPayloadAfter());
      } else {
        afterStruct =
            fillStructWithFullImage(
                scyllaCollectionSchema.afterSchema(),
                taskInfo.getPostImage(),
                includePkInPayloadAfter());
      }

      if (beforeMode == ScyllaConnectorConfig.CdcIncludeMode.ONLY_UPDATED) {
        beforeStruct =
            fillStructWithOnlyUpdatedColumns(
                scyllaCollectionSchema.beforeSchema(),
                taskInfo.getPreImage(),
                modifiedColumns,
                includePkInPayloadBefore());
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
              includePkInPayloadAfter());

      beforeStruct =
          fillBeforeStructForUpdate(
              scyllaCollectionSchema,
              taskInfo.getPreImage(),
              taskInfo.getPostImage(),
              includePkInPayloadBefore(),
              modifiedColumns);
    }

    Struct payloadKeyStruct = fillPayloadKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

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
    ConnectHeaders headers = buildPkHeaders(anyImage);

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
    Struct keyStruct = createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

    // Build before struct with PK based on configuration
    Struct beforeStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.beforeSchema(), preImage, includePkInPayloadBefore());

    Struct payloadKeyStruct = fillPayloadKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

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
    ConnectHeaders headers = buildPkHeaders(anyImage);

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
   * <p>Scylla CDC preimage contains only the old values of columns that were modified. This method
   * identifies which columns were modified by checking for non-null values in the preimage,
   * excluding primary key columns.
   *
   * @param preImage the preimage containing old values of modified columns
   * @return set of modified column names, or empty set if preImage is null
   */
  private java.util.Set<String> getModifiedColumns(RawChange preImage) {
    java.util.Set<String> modifiedColumns = new java.util.HashSet<>();
    if (preImage == null) {
      return modifiedColumns;
    }
    for (ChangeSchema.ColumnDefinition cdef : preImage.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;
      if (isPrimaryKeyColumn(cdef)) {
        continue;
      }
      String columnName = cdef.getColumnName();
      Cell cell = preImage.getCell(columnName);
      if (cell != null && cell.getAsObject() != null) {
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
  private void fillKeyStructFromImage(Struct keyStruct, RawChange image) {
    // Iterate over key schema fields in order to maintain proper PK column ordering
    for (Field field : keyStruct.schema().fields()) {
      String columnName = field.name();
      Cell cell = image.getCell(columnName);
      if (cell != null) {
        Object value = translateCellToKafka(cell);
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
      java.util.Set<String> modifiedColumns,
      boolean includePk) {
    if (image == null) {
      return null;
    }
    Struct valueStruct = new Struct(structSchema);
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;

      String columnName = cdef.getColumnName();
      Object value = translateCellToKafka(image.getCell(columnName));

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
   * @return the populated struct, or null if image is null
   */
  private Struct fillStructWithFullImage(Schema structSchema, RawChange image, boolean includePk) {
    if (image == null) {
      return null;
    }
    Struct valueStruct = new Struct(structSchema);
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;

      Object value = translateCellToKafka(image.getCell(cdef.getColumnName()));

      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          valueStruct.put(cdef.getColumnName(), value);
        }
      } else if (value != null) {
        valueStruct.put(cdef.getColumnName(), value);
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
      java.util.Set<String> modifiedColumns) {
    // If no preimage, we have no "before" data
    if (preImage == null) {
      return null;
    }

    // If no postimage, treat preimage as a full image
    if (postImage == null) {
      return fillStructWithFullImage(collectionSchema.beforeSchema(), preImage, includePk);
    }

    // Both images present - combine data from preimage and postimage
    Struct valueStruct = new Struct(collectionSchema.beforeSchema());

    // Fill from postImage - this gives us unchanged columns and primary keys
    for (ChangeSchema.ColumnDefinition cdef : postImage.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;

      String columnName = cdef.getColumnName();

      if (isPrimaryKeyColumn(cdef)) {
        if (!includePk) {
          continue;
        }
      } else if (modifiedColumns.contains(columnName)) {
        continue;
      }
      Object value = translateCellToKafka(postImage.getCell(columnName));
      valueStruct.put(columnName, value);
    }

    // Fill modified columns from preImage (which has the OLD values)
    for (String columnName : modifiedColumns) {
      Object value = translateCellToKafka(preImage.getCell(columnName));
      if (value != null) {
        valueStruct.put(columnName, value);
      }
      // If value is null, the column is not set in beforeStruct (which is correct -
      // it means the old value was null)
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

  private Object translateCellToKafka(Cell cell) {
    ChangeSchema.DataType dataType = cell.getColumnDefinition().getCdcLogDataType();

    if (cell.getAsObject() == null) {
      return null;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DECIMAL) {
      return cell.getDecimal().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UUID) {
      return cell.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.TIMEUUID) {
      return cell.getUUID().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.VARINT) {
      return cell.getVarint().toString();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.INET) {
      return cell.getInet().getHostAddress();
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DATE) {
      CqlDate cqlDate = cell.getDate();
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.clear();
      // Months start from 0 in Calendar:
      calendar.set(cqlDate.getYear(), cqlDate.getMonth() - 1, cqlDate.getDay());
      return Date.from(calendar.toInstant());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.DURATION) {
      return cell.getDuration().toString();
    }

    return cell.getAsObject();
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
   * @return ConnectHeaders containing pk.* and ck.* headers, or null if not configured
   */
  private ConnectHeaders buildPkHeaders(RawChange image) {
    if (!includePkInKafkaHeaders() || image == null) {
      return null;
    }
    ConnectHeaders headers = new ConnectHeaders();
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;

      String columnName = cdef.getColumnName();
      Object value = translateCellToKafka(image.getCell(columnName));
      if (value == null) continue;

      String headerPrefix;
      if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY) {
        headerPrefix = "pk.";
      } else if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
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
   * @return the populated key struct, or null if not configured or image is null
   */
  private Struct fillPayloadKeyStruct(Schema keySchema, RawChange image) {
    if (!includePkInPayloadKey() || image == null || keySchema == null) {
      return null;
    }
    Struct keyStruct = new Struct(keySchema);
    fillKeyStructFromImage(keyStruct, image);
    return keyStruct;
  }
}

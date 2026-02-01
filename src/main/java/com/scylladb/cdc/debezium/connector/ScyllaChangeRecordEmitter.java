package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.*;
import java.util.stream.Collectors;

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
    for (org.apache.kafka.connect.data.Field field : keyStruct.schema().fields()) {
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
    boolean legacyFormat =
        connectorConfig.getCdcOutputFormat()
            == ScyllaConnectorConfig.CdcOutputFormat.LEGACY;

    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      String columnName = cdef.getColumnName();
      if (!ScyllaSchema.isSupportedColumnSchema(image.getSchema(), cdef)) {
        continue;
      }

      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          Object value =
              translateCellToKafka(
                  getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
          valueStruct.put(columnName, value);
        }
        continue;
      }

      if (!modifiedColumns.contains(columnName)) {
        continue;
      }

      // Non-PK, modified: legacy = Cell wrapping; advanced = raw values
      if (legacyFormat) {
        if (ScyllaSchema.isNonFrozenCollection(image.getSchema(), cdef)) {
          Struct cellOrNull =
              translateNonFrozenCollectionToKafka(
                  valueStruct, image, collectionSchema.cellSchema(columnName), cdef);
          valueStruct.put(columnName, cellOrNull);
        } else {
          Object value =
              translateCellToKafka(
                  getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
          Schema cellSchema = collectionSchema.cellSchema(columnName);
          Struct cell = new Struct(cellSchema);
          cell.put(ScyllaSchema.CELL_VALUE, value);
          valueStruct.put(columnName, cell);
        }
      } else {
        if (ScyllaSchema.isNonFrozenCollection(image.getSchema(), cdef)) {
          Struct cell =
              translateNonFrozenCollectionToKafka(
                  valueStruct, image, collectionSchema.cellSchema(columnName), cdef);
          valueStruct.put(columnName, cell != null ? cell.get(ScyllaSchema.CELL_VALUE) : null);
        } else {
          Object value =
              translateCellToKafka(
                  getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
          valueStruct.put(columnName, value);
        }
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
    boolean legacyFormat =
        connectorConfig.getCdcOutputFormat()
            == ScyllaConnectorConfig.CdcOutputFormat.LEGACY;

    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      String columnName = cdef.getColumnName();
      if (!ScyllaSchema.isSupportedColumnSchema(image.getSchema(), cdef)) {
        continue;
      }

      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          Object value =
              translateCellToKafka(
                  getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
          valueStruct.put(columnName, value);
        }
        continue;
      }

      // Non-PK: legacy = Cell wrapping (for transform); advanced = raw values (master behaviour)
      if (legacyFormat) {
        if (ScyllaSchema.isNonFrozenCollection(image.getSchema(), cdef)) {
          Struct cellOrNull =
              translateNonFrozenCollectionToKafka(
                  valueStruct, image, collectionSchema.cellSchema(columnName), cdef);
          valueStruct.put(columnName, cellOrNull);
        } else {
          Object value =
              translateCellToKafka(
                  getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
          Schema cellSchema = collectionSchema.cellSchema(columnName);
          Struct cell = new Struct(cellSchema);
          cell.put(ScyllaSchema.CELL_VALUE, value);
          valueStruct.put(columnName, cell);
        }
      } else {
        // ADVANCED - direct values, no Cell wrapper
        if (ScyllaSchema.isNonFrozenCollection(image.getSchema(), cdef)) {
          Struct cell =
              translateNonFrozenCollectionToKafka(
                  valueStruct, image, collectionSchema.cellSchema(columnName), cdef);
          valueStruct.put(columnName, cell != null ? cell.get(ScyllaSchema.CELL_VALUE) : null);
        } else {
          Object value =
              translateCellToKafka(
                  getCellSafe(image, columnName), collectionSchema.cellSchema(columnName));
          valueStruct.put(columnName, value);
        }
      }
    }
    return valueStruct;
  }

    private void fillStructWithChange(ScyllaCollectionSchema schema, Struct keyStruct, Struct valueStruct, RawChange change) {
        for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
            if (!ScyllaSchema.isSupportedColumnSchema(change.getSchema(), cdef)) continue;

            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
                Object value = translateFieldToKafka(change.getCell(cdef.getColumnName()), schema.keySchema().field(cdef.getColumnName()).schema());
                valueStruct.put(cdef.getColumnName(), value);
                keyStruct.put(cdef.getColumnName(), value);
                continue;
            }

            Schema cellSchema = schema.cellSchema(cdef.getColumnName());
            if (ScyllaSchema.isNonFrozenCollection(change.getSchema(), cdef)) {
                Struct value = translateNonFrozenCollectionToKafka(valueStruct, change, cellSchema, cdef);
                valueStruct.put(cdef.getColumnName(), value);
                continue;
            }

            Schema innerSchema = cellSchema.field(ScyllaSchema.CELL_VALUE).schema();
            Object value = translateFieldToKafka(change.getCell(cdef.getColumnName()), innerSchema);
            Boolean isDeleted = this.change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean();

            if (value != null || (isDeleted != null && isDeleted)) {
                Struct cell = new Struct(cellSchema);
                cell.put(ScyllaSchema.CELL_VALUE, value);
                valueStruct.put(cdef.getColumnName(), cell);
            }
        }
    }

    private Struct translateNonFrozenCollectionToKafka(Struct valueStruct, RawChange change, Schema cellSchema, ChangeSchema.ColumnDefinition cdef) {
        Schema innerSchema = cellSchema.field(ScyllaSchema.CELL_VALUE).schema();
        Struct cell = new Struct(cellSchema);
        Struct value = new Struct(innerSchema);

        Cell elementsCell = change.getCell(cdef.getColumnName());
        Cell deletedElementsCell = change.getCell("cdc$deleted_elements_" + cdef.getColumnName());
        boolean isDeleted = Boolean.TRUE.equals(change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean());

        Object elements;
        boolean hasModified = false;
        switch (elementsCell.getDataType().getCqlType()) {
            case SET: {
                Schema elementsSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
                Schema scyllaElementsSchema = SchemaBuilder.array(elementsSchema.keySchema()).build();
                List<Object> addedElements = (List<Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema);
                List<Object> deletedElements = (List<Object>) translateFieldToKafka(deletedElementsCell, scyllaElementsSchema);
                Map<Object, Boolean> delta = new HashMap<>();
                if (addedElements != null) {
                    addedElements.forEach(element -> delta.put(element, true));
                }
                if (deletedElements != null) {
                    deletedElements.forEach(element -> delta.put(element, false));
                }

                hasModified = !delta.isEmpty();
                elements = delta;
                break;
            }
            case LIST:
            case MAP: {
                Schema elementsSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
                Schema deletedElementsScyllaSchema = SchemaBuilder.array(elementsSchema.keySchema()).optional().build();
                Map<Object, Object> addedElements = (Map<Object, Object>) ObjectUtils.defaultIfNull(translateFieldToKafka(elementsCell, elementsSchema), new HashMap<>());
                List<Object> deletedKeys = (List<Object>)translateFieldToKafka(deletedElementsCell, deletedElementsScyllaSchema);
                if (deletedKeys != null) {
                    deletedKeys.forEach((key) -> {
                        addedElements.put(key, null);
                    });
                }

                hasModified = !addedElements.isEmpty();
                elements = addedElements;
                break;
            }
            case UDT: {
                List<Short> deletedKeysList = (List<Short>) translateFieldToKafka(deletedElementsCell, SchemaBuilder.array(Schema.INT16_SCHEMA).optional().build());
                Set<Short> deletedKeys;
                if (deletedKeysList == null) {
                    deletedKeys = new HashSet<>();
                } else {
                    deletedKeys = new HashSet<>(deletedKeysList);
                }

                Map<String, Field> elementsMap = elementsCell.getUDT();
                assert elementsMap instanceof LinkedHashMap;

                Schema udtSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
                Struct udtStruct = new Struct(udtSchema);
                Short index = -1;
                for (Map.Entry<String, Field> element : elementsMap.entrySet()) {
                    index++;
                    if ((!element.getValue().isNull()) || deletedKeys.contains(index)) {
                        hasModified = true;
                        Schema fieldCellSchema = udtSchema.field(element.getKey()).schema();
                        Struct fieldCell = new Struct (fieldCellSchema);
                        if (element.getValue().isNull()) {
                            fieldCell.put(ScyllaSchema.CELL_VALUE, null);
                        } else {
                            fieldCell.put(ScyllaSchema.CELL_VALUE, translateFieldToKafka(element.getValue(), fieldCellSchema.field(ScyllaSchema.CELL_VALUE).schema()));
                        }
                        udtStruct.put(element.getKey(), fieldCell);
                    }
                }

                elements = udtStruct;
                break;
            }
            default:
                throw new RuntimeException("Unreachable");
        }

        if (!hasModified) {
            if (isDeleted) {
                cell.put(ScyllaSchema.CELL_VALUE, null);
                return cell;
            } else {
                return null;
            }
        }

        CollectionOperation mode = isDeleted ? CollectionOperation.OVERWRITE : CollectionOperation.MODIFY;
        value.put(ScyllaSchema.MODE_VALUE, mode.toString());
        value.put(ScyllaSchema.ELEMENTS_VALUE, elements);

        cell.put(ScyllaSchema.CELL_VALUE, value);
        return cell;
    }

    private Object translateFieldToKafka(Field field, Schema resultSchema) {
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
           Schema innerSchema = resultSchema.valueSchema();
           return field.getList().stream().map((element) -> this.translateFieldToKafka(element, innerSchema)).collect(Collectors.toList());
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.SET) {
           Schema innerSchema = resultSchema.valueSchema();
           return field.getSet().stream().map((element) -> this.translateFieldToKafka(element, innerSchema)).collect(Collectors.toList());
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.MAP) {
           Map<Field, Field> map = field.getMap();
           Map<Object, Object> kafkaMap = new LinkedHashMap<>();
           Schema keySchema = resultSchema.keySchema();
           Schema valueSchema = resultSchema.valueSchema();
           map.forEach((key, value) -> {
               Object kafkaKey = translateFieldToKafka(key, keySchema);
               Object kafkaValue = translateFieldToKafka(value, valueSchema);
               kafkaMap.put(kafkaKey, kafkaValue);
           });
           return kafkaMap;
       }

       if (dataType.getCqlType() == ChangeSchema.CqlType.TUPLE) {
           List<org.apache.kafka.connect.data.Field> fields_schemas = resultSchema.fields();
           Struct tupleStruct = new Struct(resultSchema);
           List<Field> tuple = field.getTuple();
           for (int i = 0; i < tuple.size(); i++) {
               tupleStruct.put("tuple_member_" + i, translateFieldToKafka(tuple.get(i), fields_schemas.get(i).schema()));
           }
           return tupleStruct;
       }

        if (dataType.getCqlType() == ChangeSchema.CqlType.UDT) {
            Struct udtStruct = new Struct(resultSchema);
            Map<String, Field> udt = field.getUDT();
            udt.forEach((name, value) -> {
                udtStruct.put(name, translateFieldToKafka(value, resultSchema.field(name).schema()));
            });
            return udtStruct;
        }

       return field.getAsObject();
    }
    Struct keyStruct = new Struct(keySchema);
    fillKeyStructFromImage(keyStruct, image, collectionSchema);
    return keyStruct;
  }
}

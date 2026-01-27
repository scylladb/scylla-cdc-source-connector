package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnType;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

  /** Creates an emitter for a CDC change, optionally paired with a preimage. */
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

  /** Returns the schema helper used to update collection schemas. */
  public ScyllaSchema getSchema() {
    return schema;
  }

  /** Maps the Scylla CDC operation type to Debezium envelope operations. */
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

  /** Read operations are not emitted by this connector. */
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
            scyllaCollectionSchema,
            taskInfo.getPostImage(),
            includePkInPayloadAfter());

    // Build before struct with PK based on configuration
    Struct beforeStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.beforeSchema(),
            scyllaCollectionSchema,
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

  /** Emits an UPDATE record, using preimage for before and postimage for after. */
  @Override
  protected void emitUpdateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    RawChange anyImage = taskInfo.getAnyImage();
    scyllaCollectionSchema = getOrCreateSchema(scyllaCollectionSchema, anyImage);

    // Build Kafka record key based on configuration
    Struct keyStruct = createKeyStruct(scyllaCollectionSchema.keySchema(), anyImage);

    // Build after struct from postimage (user must enable postimage on table)
    Struct afterStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.afterSchema(),
            scyllaCollectionSchema,
            taskInfo.getPostImage(),
            includePkInPayloadAfter());

    // Build before struct from preimage (user must enable preimage on table)
    Struct beforeStruct =
        fillStructWithFullImage(
            scyllaCollectionSchema.beforeSchema(),
            scyllaCollectionSchema,
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

  /** Emits a DELETE record, using a preimage if one was captured. */
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
            scyllaCollectionSchema.beforeSchema(),
            scyllaCollectionSchema,
            preImage,
            includePkInPayloadBefore());

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
    for (org.apache.kafka.connect.data.Field field : keyStruct.schema().fields()) {
      String columnName = field.name();
      Cell cell = image.getCell(columnName);
      if (cell != null) {
        Object value = translateCellToKafka(cell);
        keyStruct.put(columnName, value);
      }
    }
  }

  /**
   * Builds and fills a value struct with data from a full image (preimage or postimage). This
   * method handles both frozen and non-frozen collections properly.
   *
   * @param structSchema the schema to use for creating the struct (beforeSchema or afterSchema)
   * @param collectionSchema the collection schema for accessing cell schemas
   * @param image the preimage or postimage containing the full row data, or null
   * @param includePk whether to include primary key columns
   * @return the populated struct, or null if image is null
   */
  private Struct fillStructWithFullImage(
      Schema structSchema,
      ScyllaCollectionSchema collectionSchema,
      RawChange image,
      boolean includePk) {
    if (image == null) {
      return null;
    }
    Struct valueStruct = new Struct(structSchema);
    for (ChangeSchema.ColumnDefinition cdef : image.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;

      String columnName = cdef.getColumnName();

      if (isPrimaryKeyColumn(cdef)) {
        if (includePk) {
          Object value = translateCellToKafka(image.getCell(columnName));
          valueStruct.put(columnName, value);
        }
      } else {
        Schema columnSchema = collectionSchema.cellSchema(columnName);
        if (columnSchema == null) {
          continue;
        }
        if (ScyllaSchema.isNonFrozenCollection(image.getSchema(), cdef)) {
          Object value = translateNonFrozenCollectionToKafka(image, columnSchema, cdef);
          if (value != null) {
            valueStruct.put(columnName, value);
          }
        } else {
          Object value = translateCellToKafka(image.getCell(columnName));
          if (value != null) {
            valueStruct.put(columnName, value);
          }
        }
      }
    }
    return valueStruct;
  }

  /** Builds a Debezium envelope struct with optional before/after/source/timestamp fields. */
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
   * Translates non-frozen collection changes into the connector's delta representation for Kafka.
   */
  private Object translateNonFrozenCollectionToKafka(
      RawChange change, Schema valueSchema, ChangeSchema.ColumnDefinition cdef) {
    Cell elementsCell = change.getCell(cdef.getColumnName());
    Cell deletedElementsCell = change.getCell("cdc$deleted_elements_" + cdef.getColumnName());
    Cell deletedCell = change.getCell("cdc$deleted_" + cdef.getColumnName());
    boolean isDeleted = deletedCell != null && Boolean.TRUE.equals(deletedCell.getBoolean());
    boolean hasElements = elementsCell != null && elementsCell.getAsObject() != null;
    boolean hasDeletedElements =
        deletedElementsCell != null && deletedElementsCell.getAsObject() != null;

    if (!hasElements && !hasDeletedElements) {
      return null;
    }

    Object elements;
    boolean hasModified = false;
    switch (cdef.getCdcLogDataType().getCqlType()) {
      case SET:
        {
          Schema elementSchema = valueSchema.valueSchema();
          Schema scyllaElementsSchema = SchemaBuilder.array(elementSchema).optional().build();
          @SuppressWarnings("unchecked")
          var addedElements =
              hasElements
                  ? (List<Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema)
                  : null;
          @SuppressWarnings("unchecked")
          var deletedElements =
              hasDeletedElements
                  ? (List<Object>) translateFieldToKafka(deletedElementsCell, scyllaElementsSchema)
                  : null;
          var delta =
              Stream.concat(
                      Optional.ofNullable(addedElements).stream().flatMap(List::stream),
                      Optional.ofNullable(deletedElements).stream().flatMap(List::stream))
                  .collect(Collectors.toList());
          hasModified = !delta.isEmpty();
          elements = delta;
          break;
        }
      case LIST:
        {
          Schema valueElementSchema = valueSchema.valueSchema();
          Schema scyllaElementsSchema =
              SchemaBuilder.map(Schema.STRING_SCHEMA, valueElementSchema).optional().build();
          Schema deletedElementsScyllaSchema =
              SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build();

          @SuppressWarnings("unchecked")
          var addedElements =
              hasElements
                  ? (Map<Object, Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema)
                  : null;
          @SuppressWarnings("unchecked")
          var deletedKeys =
              hasDeletedElements
                  ? (List<Object>)
                      translateFieldToKafka(deletedElementsCell, deletedElementsScyllaSchema)
                  : null;
          hasModified =
              (addedElements != null && !addedElements.isEmpty())
                  || (deletedKeys != null && !deletedKeys.isEmpty());

          List<Object> delta = new ArrayList<>();
          if (addedElements != null) {
            delta.addAll(addedElements.values());
          }
          if (deletedKeys != null) {
            for (int i = 0; i < deletedKeys.size(); i++) {
              delta.add(null);
            }
          }
          elements = delta;
          break;
        }
      case MAP:
        {
          Schema entrySchema;
          Schema keySchema;
          Schema mapValueSchema;
          if (valueSchema.type() == Schema.Type.ARRAY) {
            entrySchema = valueSchema.valueSchema();
            keySchema = entrySchema.field("key").schema();
            mapValueSchema = entrySchema.field("value").schema();
          } else {
            keySchema = valueSchema.keySchema();
            mapValueSchema = valueSchema.valueSchema();
            entrySchema =
                SchemaBuilder.struct()
                    .field("key", keySchema)
                    .field("value", mapValueSchema)
                    .build();
          }
          Schema scyllaElementsSchema =
              SchemaBuilder.map(keySchema, mapValueSchema).optional().build();
          Schema deletedElementsScyllaSchema = SchemaBuilder.array(keySchema).optional().build();

          @SuppressWarnings("unchecked")
          var addedElements =
              hasElements
                  ? (Map<Object, Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema)
                  : null;
          @SuppressWarnings("unchecked")
          var deletedKeys =
              hasDeletedElements
                  ? (List<Object>)
                      translateFieldToKafka(deletedElementsCell, deletedElementsScyllaSchema)
                  : null;
          hasModified =
              (addedElements != null && !addedElements.isEmpty())
                  || (deletedKeys != null && !deletedKeys.isEmpty());

          Schema listEntrySchema = entrySchema;
          var delta =
              Stream.concat(
                      Optional.ofNullable(addedElements).stream()
                          .flatMap(map -> map.entrySet().stream())
                          .map(
                              e ->
                                  createListElementStruct(
                                      listEntrySchema, e.getKey(), e.getValue())),
                      Optional.ofNullable(deletedKeys).stream()
                          .flatMap(List::stream)
                          .map(k -> createListElementStruct(listEntrySchema, k, null)))
                  .collect(Collectors.toList());
          elements = delta;
          break;
        }
      case UDT:
        {
          @SuppressWarnings("unchecked")
          var deletedKeys =
              Optional.ofNullable(
                      deletedElementsCell != null
                          ? (List<Short>)
                              translateFieldToKafka(
                                  deletedElementsCell,
                                  SchemaBuilder.array(Schema.INT16_SCHEMA).optional().build())
                          : null)
                  .map(HashSet::new)
                  .orElseGet(HashSet::new);

          Map<String, Field> elementsMap = elementsCell.getUDT();
          if (!(elementsMap instanceof LinkedHashMap)) {
            throw new IllegalStateException(
                "Expected LinkedHashMap for UDT elements to preserve field order");
          }

          Schema udtValueSchema = valueSchema;
          if (elementsMap.size() == 1 && elementsMap.containsKey("elements")) {
            Field elementsField = elementsMap.get("elements");
            if (elementsField != null) {
              Map<String, Field> innerMap = elementsField.getUDT();
              if (innerMap != null) {
                elementsMap = innerMap;
              }
            }
            if (valueSchema.field("elements") != null) {
              udtValueSchema = valueSchema.field("elements").schema();
            }
          }

          Struct udtStruct = new Struct(udtValueSchema);
          Short index = 0;
          for (Map.Entry<String, Field> element : elementsMap.entrySet()) {
            if ((!element.getValue().isNull()) || deletedKeys.contains(index)) {
              hasModified = true;
              Schema fieldSchema = udtValueSchema.field(element.getKey()).schema();
              if (element.getValue().isNull()) {
                udtStruct.put(element.getKey(), null);
              } else {
                udtStruct.put(
                    element.getKey(), translateFieldToKafka(element.getValue(), fieldSchema));
              }
            }
            index++;
          }

          elements = udtStruct;
          break;
        }
      default:
        throw new RuntimeException("Unreachable");
    }

    if (!hasModified) {
      return null;
    }

    return elements;
  }

  /** Creates a list-entry struct for map-like collection elements. */
  private Struct createListElementStruct(Schema entrySchema, Object key, Object value) {
    Struct elementStruct = new Struct(entrySchema);
    elementStruct.put("key", key);
    elementStruct.put("value", value);
    return elementStruct;
  }

  /** Translates a Cell to Kafka representation for scalar and frozen collection types. */
  private Object translateCellToKafka(Cell cell) {
    if (cell == null || cell.getAsObject() == null) {
      return null;
    }
    return translateFieldToKafka(cell, null);
  }

  /** Converts a Scylla CDC field value into a Kafka Connect-compatible representation. */
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
      Schema innerSchema = resultSchema != null ? resultSchema.valueSchema() : null;
      return field.getList().stream()
          .map((element) -> this.translateFieldToKafka(element, innerSchema))
          .collect(Collectors.toList());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.SET) {
      Schema innerSchema = resultSchema != null ? resultSchema.valueSchema() : null;
      return field.getSet().stream()
          .map((element) -> this.translateFieldToKafka(element, innerSchema))
          .collect(Collectors.toList());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.MAP) {
      Map<Field, Field> map = field.getMap();
      if (resultSchema != null && resultSchema.type() == Schema.Type.ARRAY) {
        List<Object> entries = new ArrayList<>();
        Schema entrySchema = resultSchema.valueSchema();
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
      List<org.apache.kafka.connect.data.Field> fieldSchemas =
          resultSchema != null ? resultSchema.fields() : null;
      Struct tupleStruct = new Struct(resultSchema);
      List<Field> tuple = field.getTuple();
      for (int i = 0; i < tuple.size(); i++) {
        Schema fieldSchema = fieldSchemas != null ? fieldSchemas.get(i).schema() : null;
        tupleStruct.put("tuple_member_" + i, translateFieldToKafka(tuple.get(i), fieldSchema));
      }
      return tupleStruct;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UDT) {
      Struct udtStruct = new Struct(resultSchema);
      Map<String, Field> udt = field.getUDT();
      udt.forEach(
          (name, value) -> {
            Schema fieldSchema =
                resultSchema != null && resultSchema.field(name) != null
                    ? resultSchema.field(name).schema()
                    : null;
            udtStruct.put(name, translateFieldToKafka(value, fieldSchema));
          });
      return udtStruct;
    }

    return field.getAsObject();
  }

  // ========== PK Location Configuration Helpers ==========

  /** Checks if the column is a primary key column (partition key or clustering key). */
  private static boolean isPrimaryKeyColumn(ChangeSchema.ColumnDefinition cdef) {
    ColumnType colType = cdef.getBaseTableColumnType();
    return colType == ColumnType.PARTITION_KEY || colType == ColumnType.CLUSTERING_KEY;
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
      if (cdef.getBaseTableColumnType() == ColumnType.PARTITION_KEY) {
        headerPrefix = "pk.";
      } else if (cdef.getBaseTableColumnType() == ColumnType.CLUSTERING_KEY) {
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

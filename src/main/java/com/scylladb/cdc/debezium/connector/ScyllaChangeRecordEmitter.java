package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnKind;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import java.time.Instant;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaChangeRecordEmitter
    extends AbstractChangeRecordEmitter<ScyllaPartition, ScyllaCollectionSchema> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaChangeRecordEmitter.class);

  private final RawChange change;
  private final ScyllaSchema schema;
  private final RawChange preImage;

  public ScyllaChangeRecordEmitter(
      ScyllaPartition partition,
      RawChange preImage,
      RawChange change,
      OffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig) {
    super(partition, offsetContext, clock, connectorConfig);
    this.change = change;
    this.schema = schema;
    this.preImage = preImage;
  }

  public RawChange getChange() {
    return change;
  }

  public ScyllaSchema getSchema() {
    return schema;
  }

  @Override
  public Envelope.Operation getOperation() {
    RawChange.OperationType operationType = this.change.getOperationType();
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

  @Override
  protected void emitCreateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    scyllaCollectionSchema =
        this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

    Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
    Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
    fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

    Struct envelope;
    if (preImage != null) {
      Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
      fillStructWithChange(scyllaCollectionSchema, null, beforeStruct, preImage);
      envelope =
          generalizedEnvelope(
              scyllaCollectionSchema.getEnvelopeSchema().schema(),
              beforeStruct,
              afterStruct,
              getOffset().getSourceInfo(),
              getClock().currentTimeAsInstant(),
              Envelope.Operation.CREATE);
    } else {
      envelope =
          scyllaCollectionSchema
              .getEnvelopeSchema()
              .create(afterStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
    }

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        null);
  }

  @Override
  protected void emitUpdateRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    scyllaCollectionSchema =
        this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

    Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
    Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
    fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

    Struct envelope;
    if (preImage != null) {
      Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
      fillStructWithChange(scyllaCollectionSchema, null, beforeStruct, preImage);
      envelope =
          generalizedEnvelope(
              scyllaCollectionSchema.getEnvelopeSchema().schema(),
              beforeStruct,
              afterStruct,
              getOffset().getSourceInfo(),
              getClock().currentTimeAsInstant(),
              Envelope.Operation.UPDATE);
    } else {
      envelope =
          scyllaCollectionSchema
              .getEnvelopeSchema()
              .update(
                  null,
                  afterStruct,
                  getOffset().getSourceInfo(),
                  getClock().currentTimeAsInstant());
    }

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        null);
  }

  @Override
  protected void emitDeleteRecord(
      Receiver<ScyllaPartition> receiver, ScyllaCollectionSchema scyllaCollectionSchema)
      throws InterruptedException {
    scyllaCollectionSchema =
        this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

    Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
    Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
    if (preImage != null) {
      fillStructWithChange(scyllaCollectionSchema, keyStruct, beforeStruct, preImage);
    } else {
      fillStructWithChange(scyllaCollectionSchema, keyStruct, beforeStruct, change);
    }

    Struct envelope =
        scyllaCollectionSchema
            .getEnvelopeSchema()
            .delete(beforeStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

    receiver.changeRecord(
        getPartition(),
        scyllaCollectionSchema,
        getOperation(),
        keyStruct,
        envelope,
        getOffset(),
        null);
  }

  private void fillStructWithChange(
      ScyllaCollectionSchema schema, Struct keyStruct, Struct valueStruct, RawChange change) {
    for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
      if (!ScyllaSchema.isSupportedColumnSchema(cdef)) continue;

      if (cdef.getBaseTableColumnKind() == ColumnKind.PARTITION_KEY
          || cdef.getBaseTableColumnKind() == ColumnKind.CLUSTERING_KEY) {
        Object value =
            translateFieldToKafka(
                change.getCell(cdef.getColumnName()),
                schema.keySchema().field(cdef.getColumnName()).schema());
        valueStruct.put(cdef.getColumnName(), value);
        if (keyStruct != null) {
          keyStruct.put(cdef.getColumnName(), value);
        }
      } else {
        Schema cellSchema = schema.cellSchema(cdef.getColumnName());
        if (cellSchema == null) {
          LOGGER.warn(
              "No schema found for column '{}'. Skipping this column to avoid NullPointerException.",
              cdef.getColumnName());
          continue;
        }

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
  }

  private Struct generalizedEnvelope(
      Schema schema,
      Object before,
      Object after,
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
    if (source != null) {
      struct.put(Envelope.FieldName.SOURCE, source);
    }
    if (timestamp != null) {
      struct.put(Envelope.FieldName.TIMESTAMP, timestamp.toEpochMilli());
    }
    return struct;
  }

  private Struct translateNonFrozenCollectionToKafka(
      Struct valueStruct, RawChange change, Schema cellSchema, ChangeSchema.ColumnDefinition cdef) {
    Schema innerSchema = cellSchema.field(ScyllaSchema.CELL_VALUE).schema();
    Struct cell = new Struct(cellSchema);
    Struct value = new Struct(innerSchema);

    Cell elementsCell = change.getCell(cdef.getColumnName());
    Cell deletedElementsCell = change.getCell("cdc$deleted_elements_" + cdef.getColumnName());
    boolean isDeleted =
        Boolean.TRUE.equals(change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean());

    Object elements;
    boolean hasModified = false;
    switch (elementsCell.getDataType().getCqlType()) {
      case SET:
        {
          Schema elementsArraySchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
          Schema entrySchema = elementsArraySchema.valueSchema();
          Schema elementSchema = entrySchema.field("element").schema();
          Schema scyllaElementsSchema = SchemaBuilder.array(elementSchema).optional().build();

          @SuppressWarnings("unchecked")
          var addedElements =
              (List<Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema);
          @SuppressWarnings("unchecked")
          var deletedElements =
              deletedElementsCell != null
                  ? (List<Object>) translateFieldToKafka(deletedElementsCell, scyllaElementsSchema)
                  : null;
          var delta =
              Stream.concat(
                      Optional.ofNullable(addedElements).stream()
                          .flatMap(List::stream)
                          .map(e -> createSetElementStruct(entrySchema, e, true)),
                      Optional.ofNullable(deletedElements).stream()
                          .flatMap(List::stream)
                          .map(e -> createSetElementStruct(entrySchema, e, false)))
                  .collect(Collectors.toList());

          hasModified = !delta.isEmpty();
          elements = delta;
          break;
        }
      case LIST:
      case MAP:
        {
          Schema elementsArraySchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
          Schema entrySchema = elementsArraySchema.valueSchema();
          Schema keySchema = entrySchema.field("key").schema();
          Schema valueSchema = entrySchema.field("value").schema();
          Schema scyllaElementsSchema =
              SchemaBuilder.map(keySchema, valueSchema).optional().build();
          Schema deletedElementsScyllaSchema = SchemaBuilder.array(keySchema).optional().build();

          @SuppressWarnings("unchecked")
          var addedElements =
              (Map<Object, Object>) translateFieldToKafka(elementsCell, scyllaElementsSchema);
          @SuppressWarnings("unchecked")
          var deletedKeys =
              deletedElementsCell != null
                  ? (List<Object>)
                      translateFieldToKafka(deletedElementsCell, deletedElementsScyllaSchema)
                  : null;
          var delta =
              Stream.concat(
                      Optional.ofNullable(addedElements).stream()
                          .flatMap(map -> map.entrySet().stream())
                          .map(e -> createListElementStruct(entrySchema, e.getKey(), e.getValue())),
                      Optional.ofNullable(deletedKeys).stream()
                          .flatMap(List::stream)
                          .map(k -> createListElementStruct(entrySchema, k, null)))
                  .collect(Collectors.toList());
          hasModified = !delta.isEmpty();
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
          assert elementsMap instanceof LinkedHashMap;

          Schema udtSchema = innerSchema.field(ScyllaSchema.ELEMENTS_VALUE).schema();
          Struct udtStruct = new Struct(udtSchema);
          Short index = 0;
          for (Map.Entry<String, Field> element : elementsMap.entrySet()) {
            if ((!element.getValue().isNull()) || deletedKeys.contains(index)) {
              hasModified = true;
              Schema fieldCellSchema = udtSchema.field(element.getKey()).schema();
              Struct fieldCell = new Struct(fieldCellSchema);
              if (element.getValue().isNull()) {
                fieldCell.put(ScyllaSchema.CELL_VALUE, null);
              } else {
                fieldCell.put(
                    ScyllaSchema.CELL_VALUE,
                    translateFieldToKafka(
                        element.getValue(),
                        fieldCellSchema.field(ScyllaSchema.CELL_VALUE).schema()));
              }
              udtStruct.put(element.getKey(), fieldCell);
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
      // When there is no per-element delta, we only have the top-level
      // deleted flag for this non-frozen collection column. Scylla CDC does
      // not distinguish between an empty collection and NULL in this case
      // for non-frozen types – both appear as a "deleted" collection with no
      // element-level information. Because of this, we cannot reliably emit
      // different Kafka representations for "[]/{}" vs "null" on INSERT.
      //
      // To avoid guessing, we treat such ambiguous non-frozen collection
      // INSERTs as if the column remains NULL at the top level. This means
      // that for non-frozen collections in delta mode, the connector only
      // guarantees a distinction between:
      //   * collection was touched and has concrete element deltas
      //     (hasModified == true) – we emit OVERWRITE/MODIFY with elements
      //   * collection was fully removed on UPDATE/DELETE – we emit a Cell
      //     with CELL_VALUE == null
      //   * all other cases (including "empty" vs "null" on INSERT without
      //     element deltas) – we emit a top-level null and let consumers
      //     interpret it as "not changed / unknown".

      RawChange.OperationType operationType = change.getOperationType();

      if (operationType == RawChange.OperationType.ROW_INSERT) {
        // Ambiguous non-frozen INSERT without element deltas: keep the
        // column as a top-level null. See comment above for rationale and
        // limitations.
        return null;
      }

      if (isDeleted) {
        // Non-INSERT operations which delete the whole collection.
        cell.put(ScyllaSchema.CELL_VALUE, null);
        return cell;
      }

      // No delta and not deleted: nothing to emit.
      return null;
    }

    RawChange.OperationType operationType = change.getOperationType();
    CollectionOperation mode =
        operationType == RawChange.OperationType.ROW_INSERT
            ? CollectionOperation.OVERWRITE
            : CollectionOperation.MODIFY;
    value.put(ScyllaSchema.MODE_VALUE, mode.toString());
    value.put(ScyllaSchema.ELEMENTS_VALUE, elements);

    cell.put(ScyllaSchema.CELL_VALUE, value);
    return cell;
  }

  private Struct createListElementStruct(Schema entrySchema, Object key, Object value) {
    Struct elementStruct = new Struct(entrySchema);
    elementStruct.put("key", key);
    elementStruct.put("value", value);
    return elementStruct;
  }

  private Struct createSetElementStruct(Schema entrySchema, Object element, boolean added) {
    Struct elementStruct = new Struct(entrySchema);
    elementStruct.put("element", element);
    elementStruct.put("added", added);
    return elementStruct;
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
      return field.getList().stream()
          .map((element) -> this.translateFieldToKafka(element, innerSchema))
          .collect(Collectors.toList());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.SET) {
      Schema innerSchema = resultSchema.valueSchema();
      return field.getSet().stream()
          .map((element) -> this.translateFieldToKafka(element, innerSchema))
          .collect(Collectors.toList());
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.MAP) {
      Map<Field, Field> map = field.getMap();
      Map<Object, Object> kafkaMap = new LinkedHashMap<>();
      Schema keySchema = resultSchema.keySchema();
      Schema valueSchema = resultSchema.valueSchema();
      map.forEach(
          (key, value) -> {
            Object kafkaKey = translateFieldToKafka(key, keySchema);
            Object kafkaValue = translateFieldToKafka(value, valueSchema);
            kafkaMap.put(kafkaKey, kafkaValue);
          });
      return kafkaMap;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.TUPLE) {
      List<org.apache.kafka.connect.data.Field> fieldSchemas = resultSchema.fields();
      Struct tupleStruct = new Struct(resultSchema);
      List<Field> tuple = field.getTuple();
      for (int i = 0; i < tuple.size(); i++) {
        tupleStruct.put(
            "tuple_member_" + i, translateFieldToKafka(tuple.get(i), fieldSchemas.get(i).schema()));
      }
      return tupleStruct;
    }

    if (dataType.getCqlType() == ChangeSchema.CqlType.UDT) {
      Struct udtStruct = new Struct(resultSchema);
      Map<String, Field> udt = field.getUDT();
      udt.forEach(
          (name, value) -> {
            udtStruct.put(name, translateFieldToKafka(value, resultSchema.field(name).schema()));
          });
      return udtStruct;
    }

    return field.getAsObject();
  }
}

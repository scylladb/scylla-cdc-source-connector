package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for {@link RowKey}. */
class RowKeyTest {

  private static final TableName TABLE = new TableName("test_ks", "test_table");

  private static TaskId createTaskId(int vnodeIndex) {
    GenerationId genId = new GenerationId(new Timestamp(new Date(1000)));
    VNodeId vnodeId = new VNodeId(vnodeIndex);
    return new TaskId(genId, vnodeId, TABLE);
  }

  private static RawChange mockChange(int pk, int ck) {
    RawChange change = Mockito.mock(RawChange.class);

    ChangeSchema schema = Mockito.mock(ChangeSchema.class);
    ChangeSchema.ColumnDefinition pkCol = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    ChangeSchema.ColumnDefinition ckCol = Mockito.mock(ChangeSchema.ColumnDefinition.class);

    Mockito.when(pkCol.getColumnName()).thenReturn("pk");
    Mockito.when(pkCol.getBaseTableColumnType()).thenReturn(ChangeSchema.ColumnType.PARTITION_KEY);
    Mockito.when(ckCol.getColumnName()).thenReturn("ck");
    Mockito.when(ckCol.getBaseTableColumnType()).thenReturn(ChangeSchema.ColumnType.CLUSTERING_KEY);

    Mockito.when(schema.getNonCdcColumnDefinitions()).thenReturn(List.of(pkCol, ckCol));
    Mockito.when(change.getSchema()).thenReturn(schema);

    com.scylladb.cdc.model.worker.cql.Cell pkCell =
        Mockito.mock(com.scylladb.cdc.model.worker.cql.Cell.class);
    com.scylladb.cdc.model.worker.cql.Cell ckCell =
        Mockito.mock(com.scylladb.cdc.model.worker.cql.Cell.class);
    Mockito.when(pkCell.getAsObject()).thenReturn(pk);
    Mockito.when(ckCell.getAsObject()).thenReturn(ck);

    Mockito.when(change.getCell("pk")).thenReturn(pkCell);
    Mockito.when(change.getCell("ck")).thenReturn(ckCell);

    return change;
  }

  @Test
  void sameTaskIdAndSamePkCk_areEqual() {
    TaskId taskId = createTaskId(1);
    RawChange change1 = mockChange(1, 10);
    RawChange change2 = mockChange(1, 10);

    RowKey key1 = RowKey.from(taskId, change1);
    RowKey key2 = RowKey.from(taskId, change2);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void sameTaskIdButDifferentCk_areNotEqual() {
    TaskId taskId = createTaskId(1);
    RawChange changeA = mockChange(1, 1);
    RawChange changeB = mockChange(1, 2);

    RowKey keyA = RowKey.from(taskId, changeA);
    RowKey keyB = RowKey.from(taskId, changeB);

    assertNotEquals(keyA, keyB);
  }

  @Test
  void differentTaskIdButSamePkCk_areNotEqual() {
    TaskId taskId1 = createTaskId(1);
    TaskId taskId2 = createTaskId(2);
    RawChange change = mockChange(1, 10);

    RowKey key1 = RowKey.from(taskId1, change);
    RowKey key2 = RowKey.from(taskId2, change);

    assertNotEquals(key1, key2);
  }

  @Test
  void canBeUsedAsMapKey() {
    TaskId taskId = createTaskId(1);
    RawChange changeA = mockChange(1, 1);
    RawChange changeB = mockChange(1, 2);

    RowKey keyA = RowKey.from(taskId, changeA);
    RowKey keyB = RowKey.from(taskId, changeB);
    // Same row as A
    RowKey keyA2 = RowKey.from(taskId, mockChange(1, 1));

    java.util.Map<RowKey, String> map = new java.util.HashMap<>();
    map.put(keyA, "A");
    map.put(keyB, "B");

    assertEquals("A", map.get(keyA2));
    assertEquals("B", map.get(keyB));
    assertEquals(2, map.size());
  }
}

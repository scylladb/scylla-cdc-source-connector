package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TaskInfo implementations.
 *
 * <p>These tests verify the completion logic for different TaskInfo implementations based on
 * operation types.
 */
public class TaskInfoTest {

  // ==================== TaskInfo.Basic Tests ====================

  @Test
  void basicTaskInfo_isNotComplete_withoutChange() {
    TaskInfo taskInfo = new TaskInfo.Basic();
    assertFalse(taskInfo.isComplete());
  }

  @Test
  void basicTaskInfo_ignoresPreAndPostImages() {
    TaskInfo taskInfo = new TaskInfo.Basic();

    // Set methods should return this for chaining but values are ignored
    TaskInfo result = taskInfo.setPreImage(null);
    assertNotNull(result);

    result = taskInfo.setPostImage(null);
    assertNotNull(result);

    // Getters should return null for Basic implementation
    assertNull(taskInfo.getPreImage());
    assertNull(taskInfo.getPostImage());
  }

  @Test
  void basicTaskInfo_hasCreatedAtMillis() {
    long before = System.currentTimeMillis();
    TaskInfo taskInfo = new TaskInfo.Basic();
    long after = System.currentTimeMillis();

    assertTrue(taskInfo.getCreatedAtMillis() >= before);
    assertTrue(taskInfo.getCreatedAtMillis() <= after);
  }

  @Test
  void basicTaskInfo_returnsThisForChaining() {
    TaskInfo taskInfo = new TaskInfo.Basic();

    TaskInfo result1 = taskInfo.setPreImage(null);
    TaskInfo result2 = taskInfo.setPostImage(null);
    TaskInfo result3 = taskInfo.setChange(null);

    // All should return the same instance
    assertTrue(result1 == taskInfo);
    assertTrue(result2 == taskInfo);
    assertTrue(result3 == taskInfo);
  }

  // ==================== TaskInfo.Before Tests ====================

  @Test
  void beforeTaskInfo_isNotComplete_withoutChange() {
    TaskInfo taskInfo = new TaskInfo.Before();
    assertFalse(taskInfo.isComplete());
  }

  @Test
  void beforeTaskInfo_ignoresPostImage() {
    TaskInfo taskInfo = new TaskInfo.Before();
    taskInfo.setPostImage(null);

    assertNull(taskInfo.getPostImage());
  }

  @Test
  void beforeTaskInfo_hasCreatedAtMillis() {
    long before = System.currentTimeMillis();
    TaskInfo taskInfo = new TaskInfo.Before();
    long after = System.currentTimeMillis();

    assertTrue(taskInfo.getCreatedAtMillis() >= before);
    assertTrue(taskInfo.getCreatedAtMillis() <= after);
  }

  @Test
  void beforeTaskInfo_returnsThisForChaining() {
    TaskInfo taskInfo = new TaskInfo.Before();

    TaskInfo result1 = taskInfo.setPreImage(null);
    TaskInfo result2 = taskInfo.setPostImage(null);
    TaskInfo result3 = taskInfo.setChange(null);

    assertTrue(result1 == taskInfo);
    assertTrue(result2 == taskInfo);
    assertTrue(result3 == taskInfo);
  }

  // ==================== TaskInfo.After Tests ====================

  @Test
  void afterTaskInfo_isNotComplete_withoutChange() {
    TaskInfo taskInfo = new TaskInfo.After();
    assertFalse(taskInfo.isComplete());
  }

  @Test
  void afterTaskInfo_ignoresPreImage() {
    TaskInfo taskInfo = new TaskInfo.After();
    taskInfo.setPreImage(null);

    assertNull(taskInfo.getPreImage());
  }

  @Test
  void afterTaskInfo_hasCreatedAtMillis() {
    long before = System.currentTimeMillis();
    TaskInfo taskInfo = new TaskInfo.After();
    long after = System.currentTimeMillis();

    assertTrue(taskInfo.getCreatedAtMillis() >= before);
    assertTrue(taskInfo.getCreatedAtMillis() <= after);
  }

  @Test
  void afterTaskInfo_returnsThisForChaining() {
    TaskInfo taskInfo = new TaskInfo.After();

    TaskInfo result1 = taskInfo.setPreImage(null);
    TaskInfo result2 = taskInfo.setPostImage(null);
    TaskInfo result3 = taskInfo.setChange(null);

    assertTrue(result1 == taskInfo);
    assertTrue(result2 == taskInfo);
    assertTrue(result3 == taskInfo);
  }

  // ==================== TaskInfo.BeforeAfter Tests ====================

  @Test
  void beforeAfterTaskInfo_isNotComplete_withoutChange() {
    TaskInfo taskInfo = new TaskInfo.BeforeAfter();
    assertFalse(taskInfo.isComplete());
  }

  @Test
  void beforeAfterTaskInfo_hasCreatedAtMillis() {
    long before = System.currentTimeMillis();
    TaskInfo taskInfo = new TaskInfo.BeforeAfter();
    long after = System.currentTimeMillis();

    assertTrue(taskInfo.getCreatedAtMillis() >= before);
    assertTrue(taskInfo.getCreatedAtMillis() <= after);
  }

  @Test
  void beforeAfterTaskInfo_returnsThisForChaining() {
    TaskInfo taskInfo = new TaskInfo.BeforeAfter();

    TaskInfo result1 = taskInfo.setPreImage(null);
    TaskInfo result2 = taskInfo.setPostImage(null);
    TaskInfo result3 = taskInfo.setChange(null);

    assertTrue(result1 == taskInfo);
    assertTrue(result2 == taskInfo);
    assertTrue(result3 == taskInfo);
  }

  // ==================== CreatedAtMillis Timing Tests ====================

  @Test
  void createdAtMillis_isSetAtConstructionTime() throws InterruptedException {
    long before = System.currentTimeMillis();

    TaskInfo basic = new TaskInfo.Basic();
    Thread.sleep(10); // Small delay
    TaskInfo beforeInfo = new TaskInfo.Before();
    Thread.sleep(10);
    TaskInfo afterInfo = new TaskInfo.After();
    Thread.sleep(10);
    TaskInfo beforeAfter = new TaskInfo.BeforeAfter();

    long after = System.currentTimeMillis();

    // All should be within the time range
    assertTrue(basic.getCreatedAtMillis() >= before);
    assertTrue(basic.getCreatedAtMillis() <= after);

    assertTrue(beforeInfo.getCreatedAtMillis() >= before);
    assertTrue(beforeInfo.getCreatedAtMillis() <= after);

    assertTrue(afterInfo.getCreatedAtMillis() >= before);
    assertTrue(afterInfo.getCreatedAtMillis() <= after);

    assertTrue(beforeAfter.getCreatedAtMillis() >= before);
    assertTrue(beforeAfter.getCreatedAtMillis() <= after);

    // They should be in order (or equal due to timing)
    assertTrue(basic.getCreatedAtMillis() <= beforeInfo.getCreatedAtMillis());
    assertTrue(beforeInfo.getCreatedAtMillis() <= afterInfo.getCreatedAtMillis());
    assertTrue(afterInfo.getCreatedAtMillis() <= beforeAfter.getCreatedAtMillis());
  }

  // ==================== TaskInfo.Before Partition Delete Tests ====================

  private RawChange createMockChange(RawChange.OperationType operationType) {
    RawChange change = mock(RawChange.class);
    when(change.getOperationType()).thenReturn(operationType);
    return change;
  }

  @Test
  void beforeTaskInfo_partitionDelete_completesImmediately_whenNotWaitingForPreimage() {
    TaskInfo taskInfo = new TaskInfo.Before(false);
    RawChange partitionDelete = createMockChange(RawChange.OperationType.PARTITION_DELETE);

    taskInfo.setChange(partitionDelete);

    // Should be complete without preimage when waitPreimageForPartitionDelete=false
    assertTrue(taskInfo.isComplete());
  }

  @Test
  void beforeTaskInfo_partitionDelete_waitsForPreimage_whenWaitingEnabled() {
    TaskInfo taskInfo = new TaskInfo.Before(true);
    RawChange partitionDelete = createMockChange(RawChange.OperationType.PARTITION_DELETE);

    taskInfo.setChange(partitionDelete);

    // Should NOT be complete without preimage when waitPreimageForPartitionDelete=true
    assertFalse(taskInfo.isComplete());

    // Now set preimage
    RawChange preImage = createMockChange(RawChange.OperationType.PRE_IMAGE);
    taskInfo.setPreImage(preImage);

    // Should be complete with preimage
    assertTrue(taskInfo.isComplete());
  }

  @Test
  void beforeTaskInfo_rowDelete_alwaysWaitsForPreimage() {
    // With flag=false
    TaskInfo taskInfo1 = new TaskInfo.Before(false);
    RawChange rowDelete = createMockChange(RawChange.OperationType.ROW_DELETE);
    taskInfo1.setChange(rowDelete);
    assertFalse(taskInfo1.isComplete());

    // With flag=true
    TaskInfo taskInfo2 = new TaskInfo.Before(true);
    taskInfo2.setChange(rowDelete);
    assertFalse(taskInfo2.isComplete());
  }

  @Test
  void beforeTaskInfo_insert_neverWaitsForPreimage() {
    // With flag=false
    TaskInfo taskInfo1 = new TaskInfo.Before(false);
    RawChange insert = createMockChange(RawChange.OperationType.ROW_INSERT);
    taskInfo1.setChange(insert);
    assertTrue(taskInfo1.isComplete());

    // With flag=true
    TaskInfo taskInfo2 = new TaskInfo.Before(true);
    taskInfo2.setChange(insert);
    assertTrue(taskInfo2.isComplete());
  }

  // ==================== TaskInfo.BeforeAfter Partition Delete Tests ====================

  @Test
  void beforeAfterTaskInfo_partitionDelete_completesImmediately_whenNotWaitingForPreimage() {
    TaskInfo taskInfo = new TaskInfo.BeforeAfter(false);
    RawChange partitionDelete = createMockChange(RawChange.OperationType.PARTITION_DELETE);

    taskInfo.setChange(partitionDelete);

    // Should be complete without preimage when waitPreimageForPartitionDelete=false
    assertTrue(taskInfo.isComplete());
  }

  @Test
  void beforeAfterTaskInfo_partitionDelete_waitsForPreimage_whenWaitingEnabled() {
    TaskInfo taskInfo = new TaskInfo.BeforeAfter(true);
    RawChange partitionDelete = createMockChange(RawChange.OperationType.PARTITION_DELETE);

    taskInfo.setChange(partitionDelete);

    // Should NOT be complete without preimage when waitPreimageForPartitionDelete=true
    assertFalse(taskInfo.isComplete());

    // Now set preimage
    RawChange preImage = createMockChange(RawChange.OperationType.PRE_IMAGE);
    taskInfo.setPreImage(preImage);

    // Should be complete with preimage
    assertTrue(taskInfo.isComplete());
  }

  @Test
  void beforeAfterTaskInfo_rowDelete_alwaysWaitsForPreimage() {
    // With flag=false
    TaskInfo taskInfo1 = new TaskInfo.BeforeAfter(false);
    RawChange rowDelete = createMockChange(RawChange.OperationType.ROW_DELETE);
    taskInfo1.setChange(rowDelete);
    assertFalse(taskInfo1.isComplete());

    // With flag=true
    TaskInfo taskInfo2 = new TaskInfo.BeforeAfter(true);
    taskInfo2.setChange(rowDelete);
    assertFalse(taskInfo2.isComplete());
  }

  @Test
  void beforeAfterTaskInfo_insert_requiresPostimage() {
    TaskInfo taskInfo = new TaskInfo.BeforeAfter(false);
    RawChange insert = createMockChange(RawChange.OperationType.ROW_INSERT);

    taskInfo.setChange(insert);
    assertFalse(taskInfo.isComplete());

    RawChange postImage = createMockChange(RawChange.OperationType.POST_IMAGE);
    taskInfo.setPostImage(postImage);
    assertTrue(taskInfo.isComplete());
  }

  @Test
  void beforeAfterTaskInfo_update_requiresBothImages() {
    TaskInfo taskInfo = new TaskInfo.BeforeAfter(false);
    RawChange update = createMockChange(RawChange.OperationType.ROW_UPDATE);

    taskInfo.setChange(update);
    assertFalse(taskInfo.isComplete());

    RawChange preImage = createMockChange(RawChange.OperationType.PRE_IMAGE);
    taskInfo.setPreImage(preImage);
    assertFalse(taskInfo.isComplete());

    RawChange postImage = createMockChange(RawChange.OperationType.POST_IMAGE);
    taskInfo.setPostImage(postImage);
    assertTrue(taskInfo.isComplete());
  }
}

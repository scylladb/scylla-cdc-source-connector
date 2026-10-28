package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TaskInfo implementations: which images each keeps, and how further deltas of a
 * batch merge into the change.
 */
public class TaskInfoTest {

  private static RawChange change(RawChange.OperationType operationType) {
    RawChange change = mock(RawChange.class);
    when(change.getOperationType()).thenReturn(operationType);
    return change;
  }

  private static void assertKeepsImages(TaskInfo taskInfo, boolean pre, boolean post) {
    RawChange preImage = change(RawChange.OperationType.PRE_IMAGE);
    RawChange delta = change(RawChange.OperationType.ROW_UPDATE);
    RawChange postImage = change(RawChange.OperationType.POST_IMAGE);

    // Setters return the same instance for chaining
    assertSame(taskInfo, taskInfo.setPreImage(preImage).setChange(delta).setPostImage(postImage));

    assertSame(delta, taskInfo.getChange());
    assertSame(pre ? preImage : null, taskInfo.getPreImage());
    assertSame(post ? postImage : null, taskInfo.getPostImage());
    assertSame(post ? postImage : pre ? preImage : delta, taskInfo.getAnyImage());
  }

  @Test
  void basic_keepsOnlyTheChange() {
    assertKeepsImages(new TaskInfo.Basic(), false, false);
  }

  @Test
  void before_keepsThePreimage() {
    assertKeepsImages(new TaskInfo.Before(), true, false);
  }

  @Test
  void after_keepsThePostimage() {
    assertKeepsImages(new TaskInfo.After(), false, true);
  }

  @Test
  void beforeAfter_keepsBothImages() {
    assertKeepsImages(new TaskInfo.BeforeAfter(), true, true);
  }

  @Test
  void setChange_null_clearsTheChange() {
    TaskInfo taskInfo = new TaskInfo.Basic().setChange(change(RawChange.OperationType.ROW_INSERT));
    assertNull(taskInfo.setChange(null).getChange());
  }

  /**
   * INSERT ... USING TTL with a non-frozen collection logs INSERT, then two UPDATE deltas (#365).
   */
  @Test
  void setChange_furtherDeltasKeepTheFirstOperation() {
    for (TaskInfo taskInfo :
        new TaskInfo[] {
          new TaskInfo.Basic(),
          new TaskInfo.Before(),
          new TaskInfo.After(),
          new TaskInfo.BeforeAfter()
        }) {
      taskInfo
          .setChange(change(RawChange.OperationType.ROW_INSERT))
          .setChange(change(RawChange.OperationType.ROW_UPDATE))
          .setChange(change(RawChange.OperationType.ROW_UPDATE));
      assertEquals(RawChange.OperationType.ROW_INSERT, taskInfo.getChange().getOperationType());
    }
  }
}

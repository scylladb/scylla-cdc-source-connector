package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;

/**
 * Accumulates CDC data for a task based on configuration requirements.
 *
 * <p>Different implementations handle different combinations of preimage/postimage requirements:
 *
 * <ul>
 *   <li>{@link Basic} - Only requires the change event (default mode, no preimage/postimage)
 *   <li>{@link Before} - Requires preimage + change (when {@code cdc.include.before=full})
 *   <li>{@link After} - Requires change + postimage (when {@code cdc.include.after=full})
 *   <li>{@link BeforeAfter} - Requires all three: preimage + change + postimage (when both modes
 *       are enabled)
 * </ul>
 *
 * <p>The {@link #isComplete()} method determines when all required events have been received,
 * taking into account the operation type (e.g., INSERTs don't have preimages, DELETEs don't have
 * postimages).
 *
 * <p>Instances track creation time via {@link #getCreatedAtMillis()} to support timeout detection
 * for incomplete tasks.
 */
public interface TaskInfo {
  /**
   * Sets the main change event for this task.
   *
   * @param change the change event (INSERT, UPDATE, DELETE, or PARTITION_DELETE)
   * @return this TaskInfo for method chaining
   */
  TaskInfo setChange(RawChange change);

  /**
   * Sets the preimage event for this task.
   *
   * @param change the preimage event containing the row state before the change
   * @return this TaskInfo for method chaining
   */
  TaskInfo setPreImage(RawChange change);

  /**
   * Sets the postimage event for this task.
   *
   * @param change the postimage event containing the row state after the change
   * @return this TaskInfo for method chaining
   */
  TaskInfo setPostImage(RawChange change);

  /**
   * Returns the main change event.
   *
   * @return the change event, or null if not yet set
   */
  RawChange getChange();

  /**
   * Returns the preimage event.
   *
   * @return the preimage event, or null if not set or not applicable
   */
  RawChange getPreImage();

  /**
   * Returns the postimage event.
   *
   * @return the postimage event, or null if not set or not applicable
   */
  RawChange getPostImage();

  default RawChange getAnyImage() {
    if (getPostImage() != null) {
      return getPostImage();
    }
    if (getPreImage() != null) {
      return getPreImage();
    }
    return getChange();
  }

  /**
   * Checks if all required events have been received for this task.
   *
   * <p>The completion criteria depend on both the implementation and the operation type:
   *
   * <ul>
   *   <li>INSERT operations never require preimage (row didn't exist before)
   *   <li>DELETE operations never require postimage (row doesn't exist after)
   *   <li>UPDATE operations may require both preimage and postimage
   * </ul>
   *
   * @return true if all required events are present, false otherwise
   */
  boolean isComplete();

  /**
   * Returns the timestamp when this TaskInfo was created.
   *
   * @return creation time in milliseconds since epoch
   */
  long getCreatedAtMillis();

  /**
   * Basic TaskInfo that only requires the change event.
   *
   * <p>Used when neither preimage nor postimage is configured.
   */
  class Basic implements TaskInfo {
    private final long createdAtMillis = System.currentTimeMillis();
    private RawChange change;

    @Override
    public TaskInfo setChange(RawChange change) {
      this.change = change;
      return this;
    }

    @Override
    public TaskInfo setPreImage(RawChange change) {
      return this;
    }

    @Override
    public TaskInfo setPostImage(RawChange change) {
      return this;
    }

    @Override
    public RawChange getChange() {
      return change;
    }

    @Override
    public RawChange getPreImage() {
      return null;
    }

    @Override
    public RawChange getPostImage() {
      return null;
    }

    @Override
    public boolean isComplete() {
      return change != null;
    }

    @Override
    public long getCreatedAtMillis() {
      return createdAtMillis;
    }
  }

  /**
   * TaskInfo that requires preimage and change.
   *
   * <p>Used when {@code cdc.include.before=full} is configured. The table must have preimage
   * enabled ({@code WITH cdc = {'preimage': true}}).
   *
   * <p>Completion rules:
   *
   * <ul>
   *   <li>INSERT: Complete with just the change (no preimage exists for new rows)
   *   <li>UPDATE/DELETE: Requires both preimage and change
   * </ul>
   */
  class Before implements TaskInfo {
    private final long createdAtMillis = System.currentTimeMillis();
    private RawChange preImage;
    private RawChange change;

    @Override
    public TaskInfo setPreImage(RawChange preImage) {
      this.preImage = preImage;
      return this;
    }

    @Override
    public TaskInfo setPostImage(RawChange change) {
      return this;
    }

    @Override
    public TaskInfo setChange(RawChange change) {
      this.change = change;
      return this;
    }

    @Override
    public RawChange getChange() {
      return change;
    }

    @Override
    public RawChange getPreImage() {
      return preImage;
    }

    @Override
    public RawChange getPostImage() {
      return null;
    }

    @Override
    public boolean isComplete() {
      if (change == null) {
        return false;
      }
      // INSERT operations don't have preimage - row didn't exist before
      if (change.getOperationType() == RawChange.OperationType.ROW_INSERT) {
        return true;
      }
      // UPDATE and DELETE require preimage
      return preImage != null;
    }

    @Override
    public long getCreatedAtMillis() {
      return createdAtMillis;
    }
  }

  /**
   * TaskInfo that requires change and postimage.
   *
   * <p>Used when {@code cdc.include.after=full} is configured. The table must have postimage
   * enabled ({@code WITH cdc = {'postimage': true}}).
   *
   * <p>Completion rules:
   *
   * <ul>
   *   <li>DELETE: Complete with just the change (no postimage exists for deleted rows)
   *   <li>INSERT/UPDATE: Requires both change and postimage
   * </ul>
   */
  class After implements TaskInfo {
    private final long createdAtMillis = System.currentTimeMillis();
    private RawChange postImage;
    private RawChange change;

    @Override
    public TaskInfo setPostImage(RawChange postImage) {
      this.postImage = postImage;
      return this;
    }

    @Override
    public TaskInfo setChange(RawChange change) {
      this.change = change;
      return this;
    }

    @Override
    public TaskInfo setPreImage(RawChange change) {
      return this;
    }

    @Override
    public RawChange getChange() {
      return change;
    }

    @Override
    public RawChange getPreImage() {
      return null;
    }

    @Override
    public RawChange getPostImage() {
      return postImage;
    }

    @Override
    public boolean isComplete() {
      if (change == null) {
        return false;
      }
      // DELETE operations don't have postimage - row no longer exists after
      RawChange.OperationType op = change.getOperationType();
      if (op == RawChange.OperationType.ROW_DELETE
          || op == RawChange.OperationType.PARTITION_DELETE) {
        return true;
      }
      // INSERT and UPDATE require postimage
      return postImage != null;
    }

    @Override
    public long getCreatedAtMillis() {
      return createdAtMillis;
    }
  }

  /**
   * TaskInfo that requires preimage, change, and postimage.
   *
   * <p>Used when both {@code cdc.include.before=full} and {@code cdc.include.after=full} are
   * configured. The table must have both preimage and postimage enabled.
   *
   * <p>Completion rules:
   *
   * <ul>
   *   <li>INSERT: Requires change + postimage (no preimage for new rows)
   *   <li>DELETE: Requires preimage + change (no postimage for deleted rows)
   *   <li>UPDATE: Requires all three: preimage + change + postimage
   * </ul>
   */
  class BeforeAfter implements TaskInfo {
    private final long createdAtMillis = System.currentTimeMillis();
    private RawChange preImage;
    private RawChange postImage;
    private RawChange change;

    @Override
    public TaskInfo setPreImage(RawChange preImage) {
      this.preImage = preImage;
      return this;
    }

    @Override
    public TaskInfo setPostImage(RawChange postImage) {
      this.postImage = postImage;
      return this;
    }

    @Override
    public TaskInfo setChange(RawChange change) {
      this.change = change;
      return this;
    }

    @Override
    public RawChange getChange() {
      return change;
    }

    @Override
    public RawChange getPreImage() {
      return preImage;
    }

    @Override
    public RawChange getPostImage() {
      return postImage;
    }

    @Override
    public boolean isComplete() {
      if (change == null) {
        return false;
      }
      RawChange.OperationType op = change.getOperationType();
      switch (op) {
        case ROW_INSERT:
          // INSERT: no preimage (row didn't exist), requires postimage
          return postImage != null;
        case ROW_DELETE:
          // ROW_DELETE: no postimage (row no longer exists), requires preimage
          return preImage != null;
        case PARTITION_DELETE:
          // PARTITION_DELETE: Scylla doesn't send preimage for partition deletes,
          // even on tables with only partition key (no clustering key).
          // Complete immediately since preimage won't arrive.
          return true;
        case ROW_UPDATE:
          // UPDATE: requires both preimage and postimage
          return preImage != null && postImage != null;
        default:
          return false;
      }
    }

    @Override
    public long getCreatedAtMillis() {
      return createdAtMillis;
    }
  }
}

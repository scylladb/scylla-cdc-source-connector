package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;

/**
 * Collects the CDC log rows of one record: the change and, when configured, its preimage and
 * postimage. {@link ScyllaChangesConsumer} dispatches it when its CDC batch ends.
 *
 * <p>Implementations differ only in which images they keep:
 *
 * <ul>
 *   <li>{@link Basic} - the change only (no preimage/postimage)
 *   <li>{@link Before} - preimage + change (when {@code cdc.include.before} needs an image)
 *   <li>{@link After} - change + postimage (when {@code cdc.include.after} needs an image)
 *   <li>{@link BeforeAfter} - preimage + change + postimage
 * </ul>
 */
public interface TaskInfo {
  /**
   * Sets the main change event for this task. A further delta of the same CDC batch is merged into
   * the first one, which keeps its operation (see {@link MergedRawChange}).
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

  /** TaskInfo that keeps only the change; used when neither image is configured. */
  class Basic implements TaskInfo {
    private RawChange change;

    @Override
    public TaskInfo setChange(RawChange change) {
      this.change = MergedRawChange.merge(this.change, change);
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
  }

  /**
   * TaskInfo that keeps the preimage and the change. The table must have preimage enabled ({@code
   * WITH cdc = {'preimage': true}}); a row that did not exist before the change has none.
   */
  class Before implements TaskInfo {
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
      this.change = MergedRawChange.merge(this.change, change);
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
  }

  /**
   * TaskInfo that keeps the change and the postimage. The table must have postimage enabled ({@code
   * WITH cdc = {'postimage': true}}); a delete has none.
   */
  class After implements TaskInfo {
    private RawChange postImage;
    private RawChange change;

    @Override
    public TaskInfo setPostImage(RawChange postImage) {
      this.postImage = postImage;
      return this;
    }

    @Override
    public TaskInfo setChange(RawChange change) {
      this.change = MergedRawChange.merge(this.change, change);
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
  }

  /** TaskInfo that keeps the preimage, the change and the postimage. */
  class BeforeAfter implements TaskInfo {
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
      this.change = MergedRawChange.merge(this.change, change);
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
  }
}

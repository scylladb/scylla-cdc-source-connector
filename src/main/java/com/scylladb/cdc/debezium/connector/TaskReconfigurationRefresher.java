package com.scylladb.cdc.debezium.connector;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-requests task reconfiguration so a request Kafka Connect drops silently cannot leave the
 * connector RUNNING with no tasks.
 *
 * <p>Every {@code taskConfigs()} call runs inside a Kafka Connect retry chain that repeats on any
 * failure until it succeeds. A request is therefore only sent when no chain can still be retrying
 * because of an earlier one: no call is in progress, the last call did not fail, and Connect has
 * not called {@code taskConfigs()} for a full interval. A request that never reaches {@code
 * taskConfigs()}, because Connect skipped it or failed before, is repeated at most {@link
 * #MAX_UNANSWERED_REQUESTS} times in a row, which bounds the retry chains it can add while still
 * recovering from a skipped request. Failures after {@code taskConfigs()} returns (forwarding to
 * the leader, writing the config topic) are invisible to the connector; there only the interval
 * spaces requests.
 */
final class TaskReconfigurationRefresher implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TaskReconfigurationRefresher.class);
  private static final long MAX_CHECK_PERIOD_MS = 1000;
  // Requests in a row that may go without a taskConfigs() call before the refresher waits for one.
  static final int MAX_UNANSWERED_REQUESTS = 3;

  private final Runnable requestTaskReconfiguration;
  private final long intervalMs;
  private final LongSupplier nanoTime;
  private ScheduledExecutorService executor;

  // Guarded by this.
  private boolean closed;
  private int unansweredRequests;
  private int taskConfigsInProgress;
  private boolean lastTaskConfigsFailed;
  private long lastActivityNanos;

  TaskReconfigurationRefresher(
      Runnable requestTaskReconfiguration, long intervalMs, LongSupplier nanoTime) {
    this.requestTaskReconfiguration = requestTaskReconfiguration;
    this.intervalMs = intervalMs;
    this.nanoTime = nanoTime;
    this.lastActivityNanos = nanoTime.getAsLong();
  }

  /** Starts checking on a daemon thread. */
  void start() {
    executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "scylla-cdc-task-reconfiguration-refresher");
              t.setDaemon(true);
              return t;
            });
    long checkPeriodMs = Math.min(intervalMs, MAX_CHECK_PERIOD_MS);
    executor.scheduleWithFixedDelay(
        this::refresh, checkPeriodMs, checkPeriodMs, TimeUnit.MILLISECONDS);
  }

  /** Runs a {@code taskConfigs()} computation, recording that it is in progress and its outcome. */
  <T> T trackTaskConfigs(Supplier<T> taskConfigs) {
    synchronized (this) {
      unansweredRequests = 0;
      taskConfigsInProgress++;
    }
    boolean failed = true;
    try {
      T result = taskConfigs.get();
      failed = false;
      return result;
    } finally {
      synchronized (this) {
        taskConfigsInProgress--;
        lastTaskConfigsFailed = failed;
        lastActivityNanos = nanoTime.getAsLong();
      }
    }
  }

  /**
   * Requests task reconfiguration if no retry chain can still be pending. The request is sent while
   * holding the lock, so none is sent once {@link #close()} returns.
   */
  synchronized void refresh() {
    long now = nanoTime.getAsLong();
    if (closed
        || unansweredRequests >= MAX_UNANSWERED_REQUESTS
        || taskConfigsInProgress > 0
        || lastTaskConfigsFailed
        || now - lastActivityNanos < TimeUnit.MILLISECONDS.toNanos(intervalMs)) {
      return;
    }
    lastActivityNanos = now;
    try {
      requestTaskReconfiguration.run();
      unansweredRequests++;
    } catch (RuntimeException e) {
      // Nothing was submitted; retry after another interval.
      logger.warn("Periodic task reconfiguration request failed", e);
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      closed = true;
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}

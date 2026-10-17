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
 * failure until it succeeds, or ends silently once Connect skips it. A request is only sent when no
 * call is in progress and Connect has not called {@code taskConfigs()} for a full interval, and at
 * most {@link #MAX_REQUESTS_WITHOUT_SUCCESS} requests are sent until a {@code taskConfigs()} call
 * succeeds. That bounds the retry chains added while requests are skipped or fail before or inside
 * {@code taskConfigs()}, yet still recovers when a chain ended without succeeding. Failures after
 * {@code taskConfigs()} returns (forwarding to the leader, writing the config topic) are invisible
 * to the connector; there only the interval spaces requests.
 */
final class TaskReconfigurationRefresher implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TaskReconfigurationRefresher.class);
  private static final long MAX_CHECK_PERIOD_MS = 1000;
  // Requests that may be sent before a taskConfigs() call succeeds.
  static final int MAX_REQUESTS_WITHOUT_SUCCESS = 3;

  private final Runnable requestTaskReconfiguration;
  private final long intervalMs;
  private final LongSupplier nanoTime;
  private ScheduledExecutorService executor;

  // Guarded by this.
  private boolean closed;
  private int requestsWithoutSuccess;
  private int taskConfigsInProgress;
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
        if (!failed) {
          requestsWithoutSuccess = 0;
        }
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
        || requestsWithoutSuccess >= MAX_REQUESTS_WITHOUT_SUCCESS
        || taskConfigsInProgress > 0
        || now - lastActivityNanos < TimeUnit.MILLISECONDS.toNanos(intervalMs)) {
      return;
    }
    lastActivityNanos = now;
    try {
      requestTaskReconfiguration.run();
      requestsWithoutSuccess++;
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

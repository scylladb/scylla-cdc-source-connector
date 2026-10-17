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
 * because of an earlier one: the previous request has reached {@code taskConfigs()}, no call is in
 * progress, the last call did not fail, and Connect has not called {@code taskConfigs()} for a full
 * interval. A request that fails before reaching {@code taskConfigs()} is never followed by
 * another, so the refresher adds at most one retry chain. Failures after {@code taskConfigs()}
 * returns (forwarding to the leader, writing the config topic) are invisible to the connector;
 * there only the interval spaces requests.
 */
final class TaskReconfigurationRefresher implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TaskReconfigurationRefresher.class);
  private static final long MAX_CHECK_PERIOD_MS = 1000;

  private final Runnable requestTaskReconfiguration;
  private final long intervalMs;
  private final LongSupplier nanoTime;
  private ScheduledExecutorService executor;

  // Guarded by this.
  private boolean awaitingTaskConfigs;
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
      awaitingTaskConfigs = false;
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

  /** Requests task reconfiguration if no retry chain can still be pending. */
  void refresh() {
    synchronized (this) {
      if (awaitingTaskConfigs
          || taskConfigsInProgress > 0
          || lastTaskConfigsFailed
          || nanoTime.getAsLong() - lastActivityNanos < TimeUnit.MILLISECONDS.toNanos(intervalMs)) {
        return;
      }
      awaitingTaskConfigs = true;
    }
    try {
      requestTaskReconfiguration.run();
    } catch (RuntimeException e) {
      // Nothing was submitted, so retry after another interval.
      synchronized (this) {
        awaitingTaskConfigs = false;
        lastActivityNanos = nanoTime.getAsLong();
      }
      logger.warn("Periodic task reconfiguration request failed", e);
    }
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}

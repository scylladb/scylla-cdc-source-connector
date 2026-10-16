package com.scylladb.cdc.debezium.connector;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-requests task reconfiguration when Kafka Connect has not called {@code taskConfigs()} for a
 * full interval.
 *
 * <p>Task configs are published asynchronously, after the master discovers the CDC streams, through
 * a single {@code requestTaskReconfiguration()} call. Kafka Connect can drop that request silently
 * (e.g. while the connector is restarting during a rebalance, or when it compares against a stale
 * snapshot), leaving the connector RUNNING with no tasks. Repeating the request recovers once
 * Connect processes one of them.
 *
 * <p>Connect retries a failed reconfiguration by itself, calling {@code taskConfigs()} again with a
 * backoff capped at 60 seconds. Waiting for a full interval without such a call keeps the refresher
 * from starting new retry chains while one is still running, as long as the interval exceeds that
 * cap.
 */
final class TaskReconfigurationRefresher implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TaskReconfigurationRefresher.class);

  private final Runnable requestTaskReconfiguration;
  private final long intervalNanos;
  private final ScheduledExecutorService executor;
  private volatile long lastTaskConfigsNanos = System.nanoTime();

  TaskReconfigurationRefresher(Runnable requestTaskReconfiguration, long intervalMs) {
    this.requestTaskReconfiguration = requestTaskReconfiguration;
    this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMs);
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "scylla-cdc-task-reconfiguration-refresher");
              t.setDaemon(true);
              return t;
            });
    scheduleRefresh(intervalNanos);
  }

  /** Records that Kafka Connect called {@code taskConfigs()}, successfully or not. */
  void onTaskConfigsCalled() {
    lastTaskConfigsNanos = System.nanoTime();
  }

  private void refresh() {
    long remainingNanos = intervalNanos - (System.nanoTime() - lastTaskConfigsNanos);
    if (remainingNanos <= 0) {
      try {
        requestTaskReconfiguration.run();
      } catch (RuntimeException e) {
        // Keep the schedule alive; a later attempt can still succeed.
        logger.warn("Periodic task reconfiguration request failed", e);
      }
      remainingNanos = intervalNanos;
    }
    scheduleRefresh(remainingNanos);
  }

  private void scheduleRefresh(long delayNanos) {
    try {
      executor.schedule(this::refresh, delayNanos, TimeUnit.NANOSECONDS);
    } catch (RejectedExecutionException e) {
      // close() shut the executor down; nothing left to schedule.
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}

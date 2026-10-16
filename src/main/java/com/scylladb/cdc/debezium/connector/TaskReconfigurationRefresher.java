package com.scylladb.cdc.debezium.connector;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically asks Kafka Connect to recompute task configs.
 *
 * <p>Task configs are published asynchronously, after the master discovers the CDC streams, through
 * a single {@code requestTaskReconfiguration()} call. Kafka Connect can drop that request silently
 * (e.g. while the connector is restarting during a rebalance, or when it compares against a stale
 * snapshot), leaving the connector RUNNING with no tasks. Repeating the request makes the published
 * configs converge; Connect ignores it when the generated configs are unchanged.
 */
final class TaskReconfigurationRefresher implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TaskReconfigurationRefresher.class);

  private final ScheduledExecutorService executor;

  TaskReconfigurationRefresher(Runnable requestTaskReconfiguration, long intervalMs) {
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "scylla-cdc-task-reconfiguration-refresher");
              t.setDaemon(true);
              return t;
            });
    executor.scheduleWithFixedDelay(
        () -> {
          try {
            requestTaskReconfiguration.run();
          } catch (RuntimeException e) {
            // Keep the schedule alive; a later attempt can still succeed.
            logger.warn("Periodic task reconfiguration request failed", e);
          }
        },
        intervalMs,
        intervalMs,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}

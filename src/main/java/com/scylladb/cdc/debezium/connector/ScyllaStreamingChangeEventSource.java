package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.GroupedTasks;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaStreamingChangeEventSource
    implements StreamingChangeEventSource<ScyllaPartition, ScyllaOffsetContext> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ScyllaStreamingChangeEventSource.class);
  private final ScyllaConnectorConfig configuration;
  private ScyllaTaskContext taskContext;
  private final DatabaseSchema<CollectionId> schema;
  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final Clock clock;
  private final Duration pollInterval;

  public ScyllaStreamingChangeEventSource(
      ScyllaConnectorConfig configuration,
      ScyllaTaskContext taskContext,
      DatabaseSchema<CollectionId> schema,
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      Clock clock) {
    this.configuration = configuration;
    this.taskContext = taskContext;
    this.schema = schema;
    this.dispatcher = dispatcher;
    this.clock = clock;
    this.pollInterval = configuration.getPollInterval();
  }

  @Override
  public void execute(
      ChangeEventSourceContext context,
      ScyllaPartition partition,
      ScyllaOffsetContext offsetContext)
      throws InterruptedException {
    // Detect Scylla version for feature compatibility checks
    ScyllaVersion scyllaVersion = new ScyllaVersionChecker(configuration).getVersion();

    // Acquire a shared CQL session from the JVM-global cache. Tasks targeting the same
    // cluster with identical connection parameters will share a single session, reducing
    // the aggregate number of connections and in-flight requests to each ScyllaDB node.
    Driver3Session session = null;
    boolean acquired = false;
    try {
      session = SharedSessionCache.acquire(configuration);
      acquired = true;
      Driver3WorkerCQL cql = new Driver3WorkerCQL(session);
      RetryBackoff retryBackoff = configuration.createCDCWorkerRetryBackoff();
      ScyllaWorkerTransport workerTransport =
          new ScyllaWorkerTransport(
              context, offsetContext, dispatcher, configuration.getHeartbeatIntervalMs());

      // Create the appropriate consumer based on output format configuration
      TaskAndRawChangeConsumer changeConsumer = createChangeConsumer(offsetContext, scyllaVersion);

      WorkerConfiguration workerConfiguration =
          WorkerConfiguration.builder()
              .withTransport(workerTransport)
              .withCQL(cql)
              .withConsumer(changeConsumer)
              .withQueryTimeWindowSizeMs(configuration.getQueryTimeWindowSizeMs())
              .withConfidenceWindowSizeMs(configuration.getConfidenceWindowSizeMs())
              .withWorkerRetryBackoff(retryBackoff)
              .withMinimalWaitForWindowMs(configuration.getMinimalWaitForWindowMs())
              .build();
      var worker = new Worker(workerConfiguration);
      runWorker(context, taskContext, worker);
    } finally {
      if (acquired) {
        SharedSessionCache.release(configuration);
      }
    }
  }

  /**
   * Creates the appropriate change consumer based on the output format configuration.
   *
   * @param offsetContext the offset context
   * @param scyllaVersion the detected Scylla version, may be null
   * @return the appropriate consumer implementation
   */
  private TaskAndRawChangeConsumer createChangeConsumer(
      ScyllaOffsetContext offsetContext, ScyllaVersion scyllaVersion) {
    if (configuration.getCdcOutputFormat() == ScyllaConnectorConfig.CdcOutputFormat.LEGACY) {
      return new ScyllaChangesConsumerLegacy(
          dispatcher, offsetContext, (ScyllaSchemaLegacy) schema, clock, configuration);
    } else {
      return new ScyllaChangesConsumer(
          dispatcher, offsetContext, (ScyllaSchema) schema, clock, configuration, scyllaVersion);
    }
  }

  /**
   * Runs the CDC worker with tasks from the task context, retrying on transient errors.
   *
   * <p>Constructs {@link GroupedTasks} directly from the task context using the generation ID
   * embedded in the tasks themselves, avoiding a redundant database query. The master already
   * validated the generation when it assigned these tasks.
   *
   * <p>When multiple tasks share a CQL session (via {@link SharedSessionCache}), the connection
   * pool may temporarily become saturated, causing {@code BusyPoolException} (surfaced as {@code
   * NoHostAvailableException}). This method retries with exponential backoff on such transient
   * failures rather than immediately killing the connector task.
   *
   * <p>The number of retry attempts is controlled by the {@code worker.max.retries} configuration
   * parameter (default 20). Set to -1 for unlimited retries.
   *
   * @param context the change event source context for checking if connector is still running
   * @param taskContext the task context containing assigned tasks
   * @param worker the CDC worker
   * @throws InterruptedException if the thread is interrupted
   * @throws ConnectException if the worker fails to execute after all retries
   */
  private void runWorker(
      ChangeEventSourceContext context, ScyllaTaskContext taskContext, Worker worker)
      throws InterruptedException {
    var taskList = taskContext.getTasks();
    if (taskList.isEmpty()) {
      throw new ConnectException("No tasks assigned to worker - cannot determine generation ID");
    }
    var tasks = taskList.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    GenerationId generationId = taskList.get(0).getKey().getGenerationId();
    GroupedTasks groupedTasks = new GroupedTasks(tasks, generationId);

    RetryBackoff retryBackoff = configuration.createCDCWorkerRetryBackoff();
    int maxRetries = configuration.getMaxWorkerRetries();
    for (long attempt = 0; maxRetries < 0 || attempt < maxRetries; attempt++) {
      // Check if connector is shutting down before attempting next retry
      if (!context.isRunning()) {
        throw new InterruptedException("Connector shutdown requested during worker retry");
      }

      try {
        worker.run(groupedTasks);
        return;
      } catch (ExecutionException | RuntimeException e) {
        Throwable cause =
            (e instanceof ExecutionException && e.getCause() != null) ? e.getCause() : e;
        if (!isTransient(cause)) {
          throw new ConnectException("Failed to execute CDC worker tasks", cause);
        }
        if (maxRetries >= 0 && attempt >= maxRetries - 1) {
          throw new ConnectException(
              "Failed to execute CDC worker tasks after " + (attempt + 1) + " attempts", cause);
        }
        long backoffMs =
            retryBackoff.getRetryBackoffTimeMs((int) Math.min(attempt, Integer.MAX_VALUE));
        LOGGER.warn(
            "Transient error starting CDC worker (attempt {}), retrying in {} ms: {}",
            attempt + 1,
            backoffMs,
            cause.getMessage());
        // Log at ERROR level for concerning retry counts
        if (attempt > 0 && attempt % 10 == 0) {
          LOGGER.error(
              "CDC worker has retried {} times - possible permanent failure. Last error: {}",
              attempt,
              cause.getMessage());
        }
        try {
          Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw ie;
        }
      }
    }
  }

  /**
   * Determines whether an exception represents a transient condition that is likely to resolve on
   * retry. This includes pool exhaustion ({@code BusyPoolException}) when multiple tasks compete
   * for a shared session's connection pool, as well as temporary node unavailability.
   *
   * <p>{@code NoHostAvailableException} is considered transient only if it wraps a {@code
   * BusyPoolException}, indicating pool saturation rather than permanent cluster failure.
   */
  static boolean isTransient(Throwable t) {
    // Walk the full cause chain looking for known transient indicators
    for (Throwable current = t; current != null; current = current.getCause()) {
      // BusyPoolException is always transient (shared pool saturation)
      if (current instanceof BusyPoolException) {
        return true;
      }

      // OperationTimedOutException is usually transient
      if (current instanceof OperationTimedOutException) {
        return true;
      }

      // NoHostAvailableException: Only transient if it wraps BusyPoolException.
      // Per-host errors are stored in getErrors() map, not in the standard cause chain.
      if (current instanceof NoHostAvailableException) {
        return containsBusyPoolException((NoHostAvailableException) current);
      }
    }
    return false;
  }

  /**
   * Checks if a {@link NoHostAvailableException} wraps a {@link BusyPoolException} in its per-host
   * error map or cause chain.
   *
   * <p>The Datastax driver stores per-host errors in a {@code Map<EndPoint, Throwable>} accessible
   * via {@link NoHostAvailableException#getErrors()}, not in the standard {@code getCause()} chain.
   * This method inspects both the error map values and the cause chain.
   *
   * @param e the NoHostAvailableException to check
   * @return true if BusyPoolException is found in the per-host errors or cause chain
   */
  private static boolean containsBusyPoolException(NoHostAvailableException e) {
    // Check the per-host error map where BusyPoolException is typically stored
    Map<?, Throwable> errors = e.getErrors();
    if (errors != null) {
      for (Throwable hostError : errors.values()) {
        if (hostError instanceof BusyPoolException) {
          return true;
        }
      }
    }
    // Also walk the cause chain as a fallback
    for (Throwable current = e.getCause(); current != null; current = current.getCause()) {
      if (current instanceof BusyPoolException) {
        return true;
      }
    }
    return false;
  }
}

package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.TransportException;
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

    Driver3Session session = null;
    boolean sharedSession = false;
    try {
      if (configuration.isSharedSessionEnabled()) {
        // Tasks targeting the same cluster with identical connection parameters will share a
        // single session, reducing the aggregate number of connections and in-flight requests
        // to each ScyllaDB node.
        session = SharedSessionCache.acquire(configuration);
        sharedSession = true;
      } else {
        session = new ScyllaSessionBuilder(configuration).build();
      }

      Driver3WorkerCQL cql = new Driver3WorkerCQL(session);
      RetryBackoff workerRetryBackoff = configuration.createCDCWorkerRetryBackoff();
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
              .withWorkerRetryBackoff(workerRetryBackoff)
              .withMinimalWaitForWindowMs(configuration.getMinimalWaitForWindowMs())
              .build();
      var worker = new Worker(workerConfiguration);
      runWorker(context, taskContext, worker);
    } finally {
      if (sharedSession) {
        SharedSessionCache.release(configuration);
      } else if (session != null) {
        session.close();
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
   * Runs the CDC worker with tasks from the task context, retrying transient startup failures for
   * both shared and dedicated sessions.
   *
   * <p>Constructs {@link GroupedTasks} directly from the task context using the generation ID
   * embedded in the tasks themselves, avoiding a redundant database query. The master already
   * validated the generation when it assigned these tasks.
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
    GroupedTasks groupedTasks = createGroupedTasks(taskContext);

    RetryBackoff retryBackoff = configuration.createCDCWorkerRetryBackoff();
    int maxAttempts = configuration.getMaxWorkerAttempts();
    for (int retryCount = 0; maxAttempts < 0 || retryCount < maxAttempts; retryCount++) {
      int attempt = retryCount + 1;
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
        if (maxAttempts >= 0 && attempt >= maxAttempts) {
          throw new ConnectException(
              "Failed to execute CDC worker tasks after " + attempt + " attempts", cause);
        }
        long backoffMs = retryBackoff.getRetryBackoffTimeMs(retryCount);
        LOGGER.warn(
            "Transient error starting CDC worker (attempt {}), retrying in {} ms: {}",
            attempt,
            backoffMs,
            cause.getMessage());
        if (attempt % 10 == 0) {
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
    throw new ConnectException("Failed to execute CDC worker tasks after exhausting all retries");
  }

  private GroupedTasks createGroupedTasks(ScyllaTaskContext taskContext) {
    var taskList = taskContext.getTasks();
    if (taskList.isEmpty()) {
      throw new ConnectException("No tasks assigned to worker - cannot determine generation ID");
    }
    var tasks = taskList.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    GenerationId generationId = taskList.get(0).getKey().getGenerationId();
    return new GroupedTasks(tasks, generationId);
  }

  /**
   * Determines whether an exception represents a transient condition that is likely to resolve on
   * retry. This includes pool exhaustion ({@code BusyPoolException}) when multiple tasks compete
   * for a shared session's connection pool, as well as temporary node unavailability.
   *
   * <p>{@code NoHostAvailableException} is considered transient only if it wraps a known transient
   * per-host error such as {@code BusyPoolException}, {@code TransportException}, or {@code
   * OperationTimedOutException}. Both the unshaded DataStax driver exceptions and the shaded
   * driver3 exceptions used by {@link Driver3WorkerCQL} are recognized.
   */
  static boolean isTransient(Throwable t) {
    // Walk the full cause chain looking for known transient indicators
    for (Throwable current = t; current != null; current = current.getCause()) {
      if (isBusyPoolException(current)) {
        return true;
      }

      if (isOperationTimedOutException(current)) {
        return true;
      }

      if (isTransportException(current)) {
        return true;
      }

      if (isTransientException(current)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if a per-host error map contains a transient exception ({@link BusyPoolException},
   * {@link TransportException}, or {@link OperationTimedOutException}).
   *
   * <p>The driver stores per-host errors in a {@code Map<EndPoint, Throwable>} accessible via
   * {@link NoHostAvailableException#getErrors()}. This method inspects only the map values; nested
   * causes are checked by {@link #isTransientHostError(Throwable)}.
   *
   * @param errors the per-host errors from {@link NoHostAvailableException#getErrors()}
   * @return true if a transient exception is found in the per-host errors
   */
  private static boolean containsTransientException(Map<?, Throwable> errors) {
    if (errors != null) {
      for (Throwable hostError : errors.values()) {
        if (isTransientHostError(hostError)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isTransientHostError(Throwable hostError) {
    return isBusyPoolException(hostError)
        || isTransportException(hostError)
        || isOperationTimedOutException(hostError);
  }

  private static boolean isBusyPoolException(Throwable current) {
    return current instanceof BusyPoolException
        || current
            instanceof shaded.com.scylladb.cdc.driver3.driver.core.exceptions.BusyPoolException;
  }

  private static boolean isOperationTimedOutException(Throwable current) {
    return current instanceof OperationTimedOutException
        || current
            instanceof
            shaded.com.scylladb.cdc.driver3.driver.core.exceptions.OperationTimedOutException;
  }

  private static boolean isTransportException(Throwable current) {
    return current instanceof TransportException
        || current
            instanceof shaded.com.scylladb.cdc.driver3.driver.core.exceptions.TransportException;
  }

  private static boolean isTransientException(Throwable current) {
    if (current instanceof NoHostAvailableException) {
      return containsTransientException(((NoHostAvailableException) current).getErrors());
    }

    if (current
        instanceof
        shaded.com.scylladb.cdc.driver3.driver.core.exceptions.NoHostAvailableException) {
      return containsTransientException(
          ((shaded.com.scylladb.cdc.driver3.driver.core.exceptions.NoHostAvailableException)
                  current)
              .getErrors());
    }

    return false;
  }
}

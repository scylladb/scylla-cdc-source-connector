package com.scylladb.cdc.debezium.connector;

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
    Driver3Session session = SharedSessionCache.acquire(configuration);
    try {
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
      runWorker(session, taskContext, worker);
    } finally {
      SharedSessionCache.release(configuration);
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
   * Runs the CDC worker with tasks from the task context.
   *
   * <p>Constructs {@link GroupedTasks} directly from the task context using the generation ID
   * embedded in the tasks themselves, avoiding a redundant database query. The master already
   * validated the generation when it assigned these tasks.
   *
   * @param session the Scylla session (unused after scylla-cdc-java 1.3.11, kept for future use)
   * @param taskContext the task context containing assigned tasks
   * @param worker the CDC worker
   * @throws InterruptedException if the thread is interrupted
   * @throws ConnectException if the worker fails to execute
   */
  private void runWorker(Driver3Session session, ScyllaTaskContext taskContext, Worker worker)
      throws InterruptedException {
    try {
      var tasks =
          taskContext.getTasks().stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      GenerationId generationId = taskContext.getTasks().get(0).getKey().getGenerationId();
      worker.run(new GroupedTasks(tasks, generationId));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new ConnectException("Failed to execute CDC worker tasks", cause);
    }
  }
}

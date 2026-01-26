package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.GroupedTasks;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaStreamingChangeEventSource
    implements StreamingChangeEventSource<ScyllaPartition, ScyllaOffsetContext> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ScyllaStreamingChangeEventSource.class);
  private static final int FETCH_GROUPED_TASKS_TIMEOUT_SECONDS = 30;

  private final ScyllaConnectorConfig configuration;
  private ScyllaTaskContext taskContext;
  private final ScyllaSchema schema;
  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final Clock clock;
  private final Duration pollInterval;

  public ScyllaStreamingChangeEventSource(
      ScyllaConnectorConfig configuration,
      ScyllaTaskContext taskContext,
      ScyllaSchema schema,
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

    Driver3Session session = new ScyllaSessionBuilder(configuration).build();
    Driver3WorkerCQL cql = new Driver3WorkerCQL(session);
    RetryBackoff retryBackoff = configuration.createCDCWorkerRetryBackoff();
    ScyllaWorkerTransport workerTransport =
        new ScyllaWorkerTransport(
            context, offsetContext, dispatcher, configuration.getHeartbeatIntervalMs());
    ScyllaChangesConsumer changeConsumer =
        new ScyllaChangesConsumer(
            dispatcher, offsetContext, schema, clock, configuration, scyllaVersion);
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
  }

  /**
   * Runs the CDC worker tasks. This method blocks while waiting for grouped tasks to be fetched.
   *
   * <p><b>Note:</b> This method must be called from a blocking context. Do not call from an event
   * loop or thread pool that expects non-blocking operations, as it may block the calling thread.
   *
   * @param session the Scylla session
   * @param taskContext the task context
   * @param worker the CDC worker
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws ConnectException if fetching grouped tasks fails or times out
   */
  private void runWorker(Driver3Session session, ScyllaTaskContext taskContext, Worker worker)
      throws InterruptedException {
    try {
      GroupedTasks tasks =
          fetchGroupedTasks(session, taskContext)
              .get(FETCH_GROUPED_TASKS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      worker.run(tasks);
    } catch (TimeoutException e) {
      throw new ConnectException(
          "Timed out after "
              + FETCH_GROUPED_TASKS_TIMEOUT_SECONDS
              + " seconds while fetching grouped tasks",
          e);
    } catch (CompletionException | ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new ConnectException("Failed to execute CDC worker tasks", cause);
    }
  }

  private CompletableFuture<GroupedTasks> fetchGroupedTasks(
      Driver3Session session, ScyllaTaskContext taskContext) {
    var tasks =
        taskContext.getTasks().stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return fetchGenerationMetadata(session)
        .thenApply(generationMetadata -> new GroupedTasks(tasks, generationMetadata));
  }

  private CompletableFuture<GenerationMetadata> fetchGenerationMetadata(Driver3Session session) {
    var masterCql = new Driver3MasterCQL(session);
    return masterCql
        .fetchFirstGenerationId()
        .thenApply(
            opt ->
                opt.orElseThrow(
                    () ->
                        new IllegalStateException(
                            "No generation ID found in CDC generation table. "
                                + "This may be caused by CDC not being enabled on the relevant tables, or the CDC generation table being empty. "
                                + "Please ensure that CDC is enabled and the generation table is populated. "
                                + "Refer to the Scylla CDC documentation for setup and troubleshooting steps.")))
        .thenCompose(firstGenerationId -> masterCql.fetchGenerationMetadata(firstGenerationId));
  }
}

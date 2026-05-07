package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.cql.WorkerCQL.Reader;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.Consumer;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.WorkerTransport;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

/** Unit tests for retry handling in {@link ScyllaStreamingChangeEventSource}. */
public class ScyllaStreamingChangeEventSourceRetryTest {

  private static final String HOST = "127.0.0.1:9042";
  private static final EndPoint TEST_ENDPOINT = () -> new InetSocketAddress("127.0.0.1", 9042);

  @Test
  void runWorker_retriesTransientStartupFailure() throws Exception {
    Configuration config = createConfiguration(2);
    ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
    ScyllaTaskContext taskContext = new ScyllaTaskContext(config, createTasks());
    ScyllaStreamingChangeEventSource source = newSource(connectorConfig, taskContext);

    ChangeEventSource.ChangeEventSourceContext context =
        mock(ChangeEventSource.ChangeEventSourceContext.class);
    when(context.isRunning()).thenReturn(true);

    WorkerFixture worker = new WorkerFixture();

    assertDoesNotThrow(() -> invokeRunWorker(source, context, taskContext, worker.worker()));
    assertEquals(2, worker.prepareCalls());
  }

  @Test
  void runWorker_stopsAfterConfiguredAttemptLimit() throws Exception {
    Configuration config = createConfiguration(1);
    ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
    ScyllaTaskContext taskContext = new ScyllaTaskContext(config, createTasks());
    ScyllaStreamingChangeEventSource source = newSource(connectorConfig, taskContext);

    ChangeEventSource.ChangeEventSourceContext context =
        mock(ChangeEventSource.ChangeEventSourceContext.class);
    when(context.isRunning()).thenReturn(true);

    WorkerFixture worker = new WorkerFixture();

    ConnectException ex =
        assertThrows(
            ConnectException.class,
            () -> invokeRunWorker(source, context, taskContext, worker.worker()));
    assertInstanceOf(BusyPoolException.class, ex.getCause());
    assertEquals(1, worker.prepareCalls());
  }

  private static ScyllaStreamingChangeEventSource newSource(
      ScyllaConnectorConfig config, ScyllaTaskContext taskContext) {
    return new ScyllaStreamingChangeEventSource(config, taskContext, null, null, null);
  }

  private static Configuration createConfiguration(int maxAttempts) {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", HOST)
        .with("scylla.table.names", "ks.table")
        .with("worker.retry.backoff.base", 1)
        .with("worker.maximum.backoff", 1)
        .with("worker.jitter.percentage", 1)
        .with("worker.max.retries", maxAttempts)
        .build();
  }

  private static List<Pair<TaskId, SortedSet<StreamId>>> createTasks() {
    SortedSet<StreamId> streams = new TreeSet<>();
    TaskId taskId =
        new TaskId(
            new GenerationId(new Timestamp(new Date(0))),
            new VNodeId(0),
            new TableName("ks", "table"));
    return Collections.singletonList(Pair.of(taskId, streams));
  }

  private static void invokeRunWorker(
      ScyllaStreamingChangeEventSource source,
      ChangeEventSource.ChangeEventSourceContext context,
      ScyllaTaskContext taskContext,
      Worker worker)
      throws Exception {
    Method method =
        ScyllaStreamingChangeEventSource.class.getDeclaredMethod(
            "runWorker",
            ChangeEventSource.ChangeEventSourceContext.class,
            ScyllaTaskContext.class,
            Worker.class);
    method.setAccessible(true);
    try {
      method.invoke(source, context, taskContext, worker);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new RuntimeException(cause);
    } finally {
      worker.stop();
    }
  }

  private static final class WorkerFixture implements WorkerCQL {
    private final AtomicInteger prepareCalls = new AtomicInteger();

    Worker worker() {
      WorkerTransport transport =
          new WorkerTransport() {
            @Override
            public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
              return Collections.emptyMap();
            }

            @Override
            public void setState(TaskId task, TaskState newState) {}

            @Override
            public void updateState(TaskId task, TaskState newState) {}

            @Override
            public void moveStateToNextWindow(TaskId task, TaskState newState) {}

            @Override
            public boolean shouldStop() {
              return true;
            }
          };

      WorkerConfiguration workerConfiguration =
          WorkerConfiguration.builder()
              .withTransport(transport)
              .withCQL(this)
              .withConsumer(Consumer.syncRawChangeConsumer(rawChange -> {}))
              .withQueryTimeWindowSizeMs(1)
              .withConfidenceWindowSizeMs(1)
              .withWorkerRetryBackoff(new ExponentialRetryBackoffWithJitter(1, 1, 0.01))
              .withMinimalWaitForWindowMs(1)
              .build();
      return new Worker(workerConfiguration);
    }

    int prepareCalls() {
      return prepareCalls.get();
    }

    @Override
    public void prepare(Set<TableName> tables) throws InterruptedException, ExecutionException {
      if (prepareCalls.getAndIncrement() == 0) {
        throw new ExecutionException(new BusyPoolException(TEST_ENDPOINT, 100));
      }
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
      return CompletableFuture.completedFuture(
          () -> CompletableFuture.completedFuture(Optional.<RawChange>empty()));
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
  }
}

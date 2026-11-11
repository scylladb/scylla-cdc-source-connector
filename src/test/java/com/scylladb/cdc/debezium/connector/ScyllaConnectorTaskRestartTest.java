package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import io.debezium.config.Configuration;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.Mockito;

/** BaseSourceTask's retriable restart runs the task's preStart again before the next start. */
@Isolated
class ScyllaConnectorTaskRestartTest {

  // Loading ScyllaConnectorTask changes the global Flogger backend property, so
  // these tests run alone and put the property back
  private static final String FLOGGER_BACKEND = System.getProperty("flogger.backend_factory");

  @AfterAll
  static void restoreFloggerBackend() {
    if (FLOGGER_BACKEND == null) {
      System.clearProperty("flogger.backend_factory");
    } else {
      System.setProperty("flogger.backend_factory", FLOGGER_BACKEND);
    }
  }

  private static final TaskId TASK_ID =
      new TaskId(
          new GenerationId(new Timestamp(new Date(1000))),
          new VNodeId(0),
          new TableName("ks", "t"));
  private static final SortedSet<StreamId> STREAMS =
      new TreeSet<>(List.of(new StreamId(ByteBuffer.wrap(new byte[16]))));

  /** Fails its first start as an unreachable cluster would; the second succeeds. */
  private static final class FailingOnceTask extends ScyllaConnectorTask {
    final List<ScyllaTaskContext> contexts = new ArrayList<>();
    // What start() would build on: the task's own field, which only preStart sets
    final List<Object> startedWith = new ArrayList<>();
    int starts;

    @Override
    public ScyllaTaskContext preStart(Configuration configuration) {
      ScyllaTaskContext context = super.preStart(configuration);
      contexts.add(context);
      return context;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ChangeEventSourceCoordinator<ScyllaPartition, ScyllaOffsetContext> start(
        Configuration configuration) {
      startedWith.add(taskContextField());
      if (++starts == 1) {
        throw new RetriableException("ScyllaDB is not reachable yet");
      }
      return Mockito.mock(ChangeEventSourceCoordinator.class);
    }

    @Override
    protected List<SourceRecord> doPoll() {
      return List.of();
    }

    private Object taskContextField() {
      try {
        Field field = ScyllaConnectorTask.class.getDeclaredField("taskContext");
        field.setAccessible(true);
        return field.get(this);
      } catch (ReflectiveOperationException e) {
        throw new AssertionError(e);
      }
    }
  }

  @Test
  void retriableStartFailure_rerunsPreStartBeforeTheNextStart() throws InterruptedException {
    FailingOnceTask task = new FailingOnceTask();
    task.initialize(Mockito.mock(SourceTaskContext.class));

    task.start(
        Map.of(
            "name",
            "test-connector",
            "topic.prefix",
            "test",
            "scylla.cluster.ip.addresses",
            "127.0.0.1:9042",
            "scylla.table.names",
            "ks.t",
            "retriable.restart.connector.wait.ms",
            "1",
            ScyllaConnectorConfig.WORKER_CONFIG.name(),
            ConfigSerializerUtil.serializeTaskConfig(TASK_ID, STREAMS)));
    try {
      assertEquals(1, task.starts);
      assertEquals(1, task.contexts.size());

      // The first poll may still fall inside the 1 ms restart backoff
      for (int i = 0; i < 100 && task.starts < 2; i++) {
        task.poll();
      }

      assertEquals(2, task.starts);
      assertEquals(2, task.contexts.size());
      assertNotSame(task.contexts.get(0), task.contexts.get(1));
      assertSame(task.contexts.get(1), task.startedWith.get(1));
      assertEquals(List.of(Pair.of(TASK_ID, STREAMS)), task.contexts.get(1).getTasks());
    } finally {
      task.stop();
    }
  }
}

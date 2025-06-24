package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.transport.WorkerTransport;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScyllaWorkerTransport implements WorkerTransport {

  private final ChangeEventSource.ChangeEventSourceContext context;
  // private final ScyllaPartition partition;
  private final ScyllaOffsetContext offsetContext;
  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final Map<TaskId, Threads.Timer> heartbeatTimers;
  private final long heartbeatIntervalMs;

  public ScyllaWorkerTransport(
      ChangeEventSource.ChangeEventSourceContext context,
      ScyllaOffsetContext offsetContext,
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      long heartbeatIntervalMs) {
    this.context = context;
    // this.partition = partition;
    this.offsetContext = offsetContext;
    this.dispatcher = dispatcher;
    this.heartbeatTimers = new HashMap<>();
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  @Override
  public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
    Map<TaskId, TaskState> result = new HashMap<>();
    tasks.forEach(
        task -> {
          TaskStateOffsetContext taskStateOffsetContext =
              offsetContext.taskStateOffsetContext(task);
          TaskState taskState = taskStateOffsetContext.getTaskState();
          if (taskState != null) {
            result.put(task, taskState);
          }
        });
    return result;
  }

  @Override
  public void setState(TaskId task, TaskState newState) {
    // Already handled in consume().
  }

  @Override
  public void moveStateToNextWindow(TaskId task, TaskState newState) {
    Threads.Timer heartbeatTimer =
        heartbeatTimers.computeIfAbsent(task, (t) -> buildHeartbeatTimer());

    TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task);
    taskStateOffsetContext.dataChangeEvent(newState);
    try {
      if (heartbeatTimer.expired()) {
        dispatcher.alwaysDispatchHeartbeatEvent(
            new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
            taskStateOffsetContext);
        // Reset the timer.
        heartbeatTimers.put(task, buildHeartbeatTimer());
      }
    } catch (InterruptedException e) {
      // TODO - handle exception
    }
  }

  private Threads.Timer buildHeartbeatTimer() {
    return Threads.timer(Clock.SYSTEM, Duration.ofMillis(heartbeatIntervalMs));
  }

  @Override
  public boolean shouldStop() {
    return !context.isRunning();
  }
}

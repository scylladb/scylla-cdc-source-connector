package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.transport.WorkerTransport;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScyllaWorkerTransport implements WorkerTransport {

    private final ChangeEventSource.ChangeEventSourceContext context;
    private final ScyllaOffsetContext offsetContext;
    private final EventDispatcher<CollectionId> dispatcher;

    public ScyllaWorkerTransport(ChangeEventSource.ChangeEventSourceContext context, ScyllaOffsetContext offsetContext, EventDispatcher<CollectionId> dispatcher) {
        this.context = context;
        this.offsetContext = offsetContext;
        this.dispatcher = dispatcher;
    }

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        Map<TaskId, TaskState> result = new HashMap<>();
        tasks.forEach(task -> {
            TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task);
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
        TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task);
        taskStateOffsetContext.dataChangeEvent(newState);
        try {
            dispatcher.alwaysDispatchHeartbeatEvent(taskStateOffsetContext);
        } catch (InterruptedException e) {
            // TODO - handle exception
        }
    }

    @Override
    public boolean shouldStop() {
        return !context.isRunning();
    }
}

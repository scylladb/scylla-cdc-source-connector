package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ScyllaStreamingChangeEventSource implements StreamingChangeEventSource<ScyllaPartition, ScyllaOffsetContext> {
    private final ScyllaConnectorConfig configuration;
    private ScyllaTaskContext taskContext;
    private final ScyllaSchema schema;
    private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final Duration pollInterval;

    public ScyllaStreamingChangeEventSource(ScyllaConnectorConfig configuration, ScyllaTaskContext taskContext,
                                            ScyllaSchema schema,
                                            EventDispatcher<ScyllaPartition, CollectionId> dispatcher, Clock clock) {
        this.configuration = configuration;
        this.taskContext = taskContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.pollInterval = configuration.getPollInterval();
    }

    @Override
    public void execute(ChangeEventSourceContext context, ScyllaPartition partition, ScyllaOffsetContext offsetContext) throws InterruptedException {
        Driver3Session session = new ScyllaSessionBuilder(configuration).build();
        Driver3WorkerCQL cql = new Driver3WorkerCQL(session);
        ScyllaWorkerTransport workerTransport = new ScyllaWorkerTransport(context, offsetContext, dispatcher, configuration.getHeartbeatIntervalMs());
        ScyllaChangesConsumer changeConsumer = new ScyllaChangesConsumer(dispatcher, offsetContext, schema, clock, configuration);
        WorkerConfiguration workerConfiguration = WorkerConfiguration.builder()
                .withTransport(workerTransport)
                .withCQL(cql)
                .withConsumer(changeConsumer)
                .withQueryTimeWindowSizeMs(configuration.getQueryTimeWindowSizeMs())
                .withConfidenceWindowSizeMs(configuration.getConfidenceWindowSizeMs())
                .build();
        Worker worker = new Worker(workerConfiguration);
        try {
            worker.run(taskContext.getTasks().stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        } catch (ExecutionException e) {
            // TODO - throw user-friendly error and do proper validation
        }
    }
}

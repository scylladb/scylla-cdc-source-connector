package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.model.worker.Worker;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Driver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ScyllaStreamingChangeEventSource implements StreamingChangeEventSource {
    private final ScyllaConnectorConfig configuration;
    private ScyllaTaskContext taskContext;
    private final ScyllaOffsetContext offsetContext;
    private final ScyllaSchema schema;
    private final EventDispatcher<CollectionId> dispatcher;
    private final Clock clock;
    private final Duration pollInterval;

    public ScyllaStreamingChangeEventSource(ScyllaConnectorConfig configuration, ScyllaTaskContext taskContext, ScyllaOffsetContext offsetContext, ScyllaSchema schema, EventDispatcher<CollectionId> dispatcher, Clock clock) {
        this.configuration = configuration;
        this.taskContext = taskContext;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.pollInterval = configuration.getPollInterval();
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        Driver3Session session = new ScyllaSessionBuilder(configuration).build();
        Driver3WorkerCQL cql = new Driver3WorkerCQL(session);
        ScyllaWorkerTransport workerTransport = new ScyllaWorkerTransport(context, offsetContext, dispatcher, configuration.getHeartbeatIntervalMs());
        ScyllaChangesConsumer changeConsumer = new ScyllaChangesConsumer(dispatcher, offsetContext, schema, clock, configuration.getPreimagesEnabled(), configuration.getPostimagesEnabled());
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

package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;

/**
 * A change consumer the worker transport tells when a task's window ends, so a consumer that
 * buffers rows can dispatch them before the window checkpoint passes them.
 */
interface WindowAwareChangeConsumer
    extends TaskAndRawChangeConsumer, ScyllaWorkerTransport.WindowEndListener {}

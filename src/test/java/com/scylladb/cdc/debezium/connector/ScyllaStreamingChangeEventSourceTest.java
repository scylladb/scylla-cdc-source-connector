package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Unit tests for transient exception detection in ScyllaStreamingChangeEventSource. */
public class ScyllaStreamingChangeEventSourceTest {

  private static final EndPoint TEST_ENDPOINT = () -> new InetSocketAddress("127.0.0.1", 9042);

  @Nested
  class IsTransientTests {

    @Test
    void busyPoolException_isTransient() {
      BusyPoolException ex = new BusyPoolException(TEST_ENDPOINT, 100);
      assertTrue(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void operationTimedOutException_isTransient() {
      OperationTimedOutException ex = new OperationTimedOutException(TEST_ENDPOINT);
      assertTrue(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void noHostAvailableException_withBusyPoolInErrors_isTransient() {
      Map<EndPoint, Throwable> errors = new HashMap<>();
      errors.put(TEST_ENDPOINT, new BusyPoolException(TEST_ENDPOINT, 100));
      NoHostAvailableException ex = new NoHostAvailableException(errors);
      assertTrue(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void noHostAvailableException_withoutBusyPool_isNotTransient() {
      Map<EndPoint, Throwable> errors = new HashMap<>();
      errors.put(TEST_ENDPOINT, new RuntimeException("Connection refused"));
      NoHostAvailableException ex = new NoHostAvailableException(errors);
      assertFalse(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void noHostAvailableException_emptyErrors_isNotTransient() {
      NoHostAvailableException ex = new NoHostAvailableException(Collections.emptyMap());
      assertFalse(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void busyPoolException_wrappedInRuntimeException_isTransient() {
      BusyPoolException busyPool = new BusyPoolException(TEST_ENDPOINT, 100);
      Exception wrapper = new RuntimeException("wrapped", busyPool);
      assertTrue(ScyllaStreamingChangeEventSource.isTransient(wrapper));
    }

    @Test
    void operationTimedOutException_wrappedInCauseChain_isTransient() {
      OperationTimedOutException timeout = new OperationTimedOutException(TEST_ENDPOINT);
      Exception wrapper = new RuntimeException("wrapper", timeout);
      assertTrue(ScyllaStreamingChangeEventSource.isTransient(wrapper));
    }

    @Test
    void genericRuntimeException_isNotTransient() {
      RuntimeException ex = new RuntimeException("something broke");
      assertFalse(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void nullPointerException_isNotTransient() {
      NullPointerException ex = new NullPointerException("null");
      assertFalse(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void illegalStateException_isNotTransient() {
      IllegalStateException ex = new IllegalStateException("bad state");
      assertFalse(ScyllaStreamingChangeEventSource.isTransient(ex));
    }

    @Test
    void noHostAvailableException_withMultipleErrors_oneBusyPool_isTransient() {
      Map<EndPoint, Throwable> errors = new HashMap<>();
      EndPoint ep1 = () -> new InetSocketAddress("10.0.0.1", 9042);
      EndPoint ep2 = () -> new InetSocketAddress("10.0.0.2", 9042);
      errors.put(ep1, new RuntimeException("Connection refused"));
      errors.put(ep2, new BusyPoolException(ep2, 100));
      NoHostAvailableException ex = new NoHostAvailableException(errors);
      assertTrue(ScyllaStreamingChangeEventSource.isTransient(ex));
    }
  }
}

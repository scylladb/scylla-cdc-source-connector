package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

/**
 * Base class for Scylla types integration tests. Provides common infrastructure for test setup,
 * teardown, and Kafka consumer management.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesIT<K, V> extends AbstractContainerBaseIT {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final Object DDL_LOCK = new Object();

  static Session session;
  static int SessionCount;
  private static final Object SESSION_LOCK = new Object();

  private KafkaConsumer<K, V> consumer;

  /** Returns the CQL CREATE TABLE statement (without "IF NOT EXISTS" and CDC options). */
  protected abstract String createTableCql(String tableName);

  /** Builds and returns a Kafka consumer for the given connector and table. */
  abstract KafkaConsumer<K, V> buildConsumer(String connectorName, String tableName);

  /** Waits for and asserts expected Kafka messages. */
  void waitAndAssert(KafkaConsumer<K, V> consumer, String[] expected) {
    List<ConsumerRecord<K, V>> actual = new ArrayList<>();
    KafkaConnectUtils.waitForConsumerAssignment(consumer);

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < KafkaConnectUtils.CONSUMER_TIMEOUT) {
      consumer.poll(Duration.ofSeconds(5)).forEach(actual::add);
      if (expected.length <= actual.size()) {
        break;
      }
    }

    List<String> errors = new ArrayList<>();
    assertEquals(expected.length, actual.size());

    for (int i = 0; i < expected.length; i++) {
      String exp = expected[i];

      V value = actual.get(i).value();

      String got;
      if (value instanceof String) {
        got = (String) value;
      } else if (value == null) {
        got = null;
      } else {
        got = value.toString();
      }

      if (exp == null || got == null) {
        if (exp != got) { // reference check is fine here: null vs non-null
          errors.add("Record[" + i + "] mismatch:\nexpected: " + exp + "\nactual:   " + got);
        }
        continue;
      }

      try {
        JSONAssert.assertEquals(exp, got, JSONCompareMode.LENIENT);
      } catch (AssertionError e) {
        errors.add(
            "Record["
                + i
                + "] mismatch:\nexpected: "
                + exp
                + "\nactual:   "
                + got
                + "\n"
                + e.getMessage());
      } catch (Exception e) {
        errors.add(
            "Record["
                + i
                + "] JSON compare failed:\nexpected: "
                + exp
                + "\nactual:   "
                + got
                + "\n"
                + e);
      }
    }

    if (!errors.isEmpty()) {
      Assertions.fail(String.join("\n\n", errors));
    }
  }

  @BeforeAll
  public static synchronized void setupSuite() {
    synchronized (SESSION_LOCK) {
      if (SessionCount == 0 && session == null) {
        session =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build()
                .connect();
      }
      SessionCount++;
    }
  }

  @AfterAll
  public static void cleanupSuite() {
    synchronized (SESSION_LOCK) {
      SessionCount--;
      if (SessionCount == 0 && session != null) {
        session.close();
        session = null;
      }
    }
  }

  @BeforeEach
  void setupTest(TestInfo testInfo) {
    synchronized (DDL_LOCK) {
      session.execute(
          "CREATE TABLE IF NOT EXISTS "
              + keyspaceTableName(testInfo)
              + " "
              + createTableCql(keyspaceTableName(testInfo))
              + " WITH cdc = {'enabled':true}");
    }

    consumer = buildConsumer(connectorName(testInfo), keyspaceTableName(testInfo));
  }

  @AfterEach
  public void cleanUp(TestInfo testInfo) {
    if (consumer != null) {
      consumer.close();
      consumer = null;
    }
    try {
      KafkaConnectUtils.removeConnector(connectorName(testInfo));
    } catch (Exception e) {
      throw new RuntimeException("Failed to remove connector: " + connectorName(testInfo), e);
    }
    synchronized (DDL_LOCK) {
      if (session != null && !session.isClosed()) {
        session.execute("DROP TABLE IF EXISTS " + keyspaceTableName(testInfo));
      } else {
        logger.atWarning().log(
            "Skipping table cleanup because session is closed: %s", keyspaceTableName(testInfo));
      }
    }
  }

  protected void truncateTables(TestInfo testInfo) {
    session.execute("TRUNCATE TABLE " + keyspaceTableName(testInfo));
    session.execute("TRUNCATE TABLE " + cdcLogTableName(testInfo));
  }

  private String cdcLogTableName(TestInfo testInfo) {
    return keyspaceName(testInfo) + "." + tableName(testInfo) + "_scylla_cdc_log";
  }

  protected KafkaConsumer<K, V> getConsumer() {
    return consumer;
  }

  public static String expectedRecord(
      TestInfo testInfo, String op, String beforeJson, String afterJson) {
    return """
        {
          "before": %s,
          "after": %s,
          "op": "%s",
          "source": {
            "connector": "scylla",
            "name": "%s",
            "snapshot": "false",
            "db": "%s",
            "keyspace_name": "%s",
            "table_name": "%s"
          }
        }
        """
        .formatted(
            beforeJson,
            afterJson,
            op,
            connectorName(testInfo),
            keyspaceName(testInfo),
            keyspaceName(testInfo),
            tableName(testInfo));
  }
}

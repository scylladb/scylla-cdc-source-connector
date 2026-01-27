package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

/**
 * Base class for Scylla types integration tests. Provides common infrastructure for test setup,
 * teardown, and Kafka consumer management.
 *
 * <p>Uses PER_CLASS lifecycle to run a single connector per test class with test isolation via
 * unique primary keys. A background subscriber continuously polls messages into a shared list.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public abstract class ScyllaTypesIT<K, V> extends AbstractContainerBaseIT {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final Object DDL_LOCK = new Object();

  static Cluster cluster;
  static Session session;
  private static final Object SESSION_LOCK = new Object();

  private final AtomicInteger pkTicket = new AtomicInteger(1);
  private final List<ConsumerRecord<K, V>> receivedRecords =
      Collections.synchronizedList(new ArrayList<>());

  private KafkaConsumer<K, V> consumer;
  private Thread pollingThread;
  private volatile boolean pollingRunning = true;

  private final ThreadLocal<Integer> recordsIndexBeforeTest = new ThreadLocal<>();

  private String suiteConnectorName;
  private String suiteKeyspaceTableName;

  /** Returns the CQL CREATE TABLE statement (without "IF NOT EXISTS" and CDC options). */
  protected abstract String createTableCql(String tableName);

  /** Builds and returns a Kafka consumer for the given connector and table. */
  abstract KafkaConsumer<K, V> buildConsumer(String connectorName, String tableName);

  /**
   * Called before table creation to allow subclasses to create custom types (UDTs, etc.). Override
   * this method to create any types needed by the table definition.
   *
   * @param keyspaceName the keyspace name where types should be created
   */
  protected void createTypesBeforeTable(String keyspaceName) {
    // Default implementation does nothing
  }

  /**
   * Extracts the primary key (id) from a Kafka record value. Subclasses must implement this to
   * support PK-based filtering.
   */
  protected abstract int extractPkFromValue(V value);

  /**
   * Extracts the primary key (id) from a Kafka record key. Subclasses must implement this to
   * support PK-based filtering for tombstone records (where value is null).
   */
  protected abstract int extractPkFromKey(K key);

  /** Returns the connector name for this test suite. */
  protected String getSuiteConnectorName() {
    return suiteConnectorName;
  }

  /** Returns the keyspace.table name for this test suite. */
  protected String getSuiteKeyspaceTableName() {
    return suiteKeyspaceTableName;
  }

  /** Returns the keyspace name for this test suite. */
  protected String getSuiteKeyspaceName() {
    int dotIndex = suiteKeyspaceTableName.indexOf('.');
    return suiteKeyspaceTableName.substring(0, dotIndex);
  }

  /** Returns the table name for this test suite. */
  protected String getSuiteTableName() {
    int dotIndex = suiteKeyspaceTableName.indexOf('.');
    return suiteKeyspaceTableName.substring(dotIndex + 1);
  }

  /**
   * Waits for and asserts expected Kafka messages for the given PK. Filters received records to
   * only match records with the specified primary key.
   *
   * @param pk the primary key to filter records by
   * @param expected the expected JSON records
   */
  void waitAndAssert(int pk, String[] expected) {
    List<ConsumerRecord<K, V>> matchingRecords = new ArrayList<>();
    int startIndex = recordsIndexBeforeTest.get();

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < KafkaConnectUtils.CONSUMER_TIMEOUT) {
      matchingRecords.clear();
      synchronized (receivedRecords) {
        for (int i = startIndex; i < receivedRecords.size(); i++) {
          ConsumerRecord<K, V> record = receivedRecords.get(i);
          V value = record.value();
          if (value == null) {
            // Tombstone records - check key to determine which test owns this record
            int keyPk = extractPkFromKey(record.key());
            if (keyPk == pk) {
              matchingRecords.add(record);
            }
          } else {
            int recordPk = extractPkFromValue(value);
            if (recordPk == pk) {
              matchingRecords.add(record);
            }
          }
        }
      }
      if (matchingRecords.size() >= expected.length) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    List<String> errors = new ArrayList<>();
    assertEquals(
        expected.length,
        matchingRecords.size(),
        "Expected " + expected.length + " records for PK " + pk);

    for (int i = 0; i < expected.length; i++) {
      String exp = expected[i];
      V value = matchingRecords.get(i).value();

      String got;
      if (value instanceof String) {
        got = (String) value;
      } else if (value == null) {
        got = null;
      } else {
        got = value.toString();
      }

      if (exp == null || got == null) {
        if (exp != got) {
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
  public void setupSuite(TestInfo testInfo) {
    synchronized (SESSION_LOCK) {
      if (cluster == null || cluster.isClosed()) {
        cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        session = cluster.connect();
      }
    }

    Assertions.assertNotNull(session, "Session was not initialized");

    // Use class-based naming for suite-level connector and table
    // Constants: MAX_CONNECTOR_NAME_LENGTH=80, MAX_TABLE_NAME_LENGTH=48
    String className = simplifyName(testInfo.getTestClass().orElseThrow().getName());
    suiteConnectorName = trimWithHash(className, 80);
    String keyspace = trimWithHash(className, 48).toLowerCase(java.util.Locale.ROOT);
    String table = "types_test";
    suiteKeyspaceTableName = keyspace + "." + table;

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + getSuiteKeyspaceName()
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

    // Allow subclasses to create custom types (UDTs) before table creation
    createTypesBeforeTable(getSuiteKeyspaceName());

    synchronized (DDL_LOCK) {
      session.execute(
          "CREATE TABLE IF NOT EXISTS "
              + suiteKeyspaceTableName
              + " "
              + createTableCql(suiteKeyspaceTableName)
              + " WITH cdc = {'enabled':true, 'preimage':true, 'postimage':true}");
    }

    consumer = buildConsumer(suiteConnectorName, suiteKeyspaceTableName);
    KafkaConnectUtils.waitForConsumerAssignment(consumer);

    pollingRunning = true;
    pollingThread =
        new Thread(
            () -> {
              while (pollingRunning) {
                try {
                  consumer.poll(Duration.ofSeconds(1)).forEach(receivedRecords::add);
                } catch (Exception e) {
                  if (pollingRunning) {
                    logger.atWarning().withCause(e).log("Error polling consumer");
                  }
                }
              }
            },
            "kafka-polling-" + suiteConnectorName);
    pollingThread.setDaemon(true);
    pollingThread.start();

    logger.atInfo().log(
        "Started connector %s for table %s with background polling",
        suiteConnectorName, suiteKeyspaceTableName);
  }

  @AfterAll
  public void cleanupSuite() {
    pollingRunning = false;
    if (pollingThread != null) {
      try {
        pollingThread.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (consumer != null) {
      consumer.close();
      consumer = null;
    }

    try {
      KafkaConnectUtils.removeConnector(suiteConnectorName);
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Failed to remove connector: %s", suiteConnectorName);
    }

    synchronized (DDL_LOCK) {
      if (session != null && !session.isClosed() && suiteKeyspaceTableName != null) {
        session.execute("DROP TABLE IF EXISTS " + suiteKeyspaceTableName);
      }
    }

    // Note: We intentionally don't close the cluster/session here.
    // With parallel test execution, one class's @AfterAll could close the session
    // while another class is still running tests. The cluster/session will be
    // cleaned up when the JVM exits.

    logger.atInfo().log("Cleaned up connector %s", suiteConnectorName);
  }

  @BeforeEach
  void setupRecordsIndex() {
    recordsIndexBeforeTest.set(receivedRecords.size());
  }

  /** Reserves and returns a unique primary key for a test. */
  protected int reservePk() {
    int pk = pkTicket.getAndIncrement();
    logger.atFine().log(
        "Test reserved PK %d, starting from record index %d", pk, recordsIndexBeforeTest.get());
    return pk;
  }

  /** Returns the expected source JSON object for use in test expectations. */
  public String expectedSource() {
    return """
        {
          "connector": "scylla",
          "name": "%s",
          "snapshot": "false",
          "db": "%s",
          "keyspace_name": "%s",
          "table_name": "%s"
        }"""
        .formatted(
            suiteConnectorName,
            getSuiteKeyspaceName(),
            getSuiteKeyspaceName(),
            getSuiteTableName());
  }

  public String expectedRecord(String op, String beforeJson, String afterJson) {
    return expectedRecord(op, beforeJson, afterJson, expectedKey());
  }

  public String expectedRecord(String op, String beforeJson, String afterJson, String keyJson) {
    return """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "%s",
          "source": %s
        }
        """
        .formatted(beforeJson, afterJson, keyJson, op, expectedSource());
  }

  /**
   * Returns the expected key JSON for the current test's primary key. Subclasses should override
   * this to provide the appropriate key structure.
   */
  protected String expectedKey() {
    return "null";
  }
}

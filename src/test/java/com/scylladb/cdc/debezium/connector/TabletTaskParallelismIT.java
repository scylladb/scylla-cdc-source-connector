package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** End-to-end coverage for per-stream tablet tasks and mixed table generations. */
public class TabletTaskParallelismIT extends AbstractContainerBaseIT {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void createsMultipleTasksForOneTabletTable(TestInfo testInfo) throws Exception {
    String connectorName = connectorName(testInfo);
    String keyspace = "single_tablet_parallelism";
    String table = "source_table";
    boolean connectorRegistrationAttempted = false;

    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect()) {
      session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
      try {
        session.execute(
            "CREATE KEYSPACE "
                + keyspace
                + " WITH replication = {'class': 'NetworkTopologyStrategy', "
                + "'replication_factor': 1} AND tablets = {'initial': 8}");
        session.execute(
            "CREATE TABLE "
                + keyspace
                + "."
                + table
                + " (id int PRIMARY KEY, value text) WITH cdc = {'enabled': true}");
      } catch (InvalidQueryException e) {
        abort("ScyllaDB version does not support CDC on tablet keyspaces: " + e.getMessage());
      }

      Properties configuration = KafkaConnectUtils.createCommonConnectorProperties();
      configuration.put("name", connectorName);
      configuration.put("topic.prefix", connectorName);
      configuration.put("scylla.table.names", keyspace + "." + table);
      configuration.put("tasks.max", "2");
      connectorRegistrationAttempted = true;
      KafkaConnectUtils.registerConnector(configuration, connectorName);

      awaitRunningTasks(connectorName, 2, Duration.ofSeconds(30));
    } finally {
      if (connectorRegistrationAttempted) {
        KafkaConnectUtils.removeConnector(connectorName);
      }
      try (Cluster cluster =
              Cluster.builder()
                  .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                  .withPort(scyllaDBContainer.getMappedPort(9042))
                  .build();
          Session session = cluster.connect()) {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
      }
    }
  }

  @Test
  void consumesTwoTabletTablesWithDifferentGenerationsInOneTask(TestInfo testInfo)
      throws Exception {
    String connectorName = connectorName(testInfo);
    String keyspace = "tablet_parallelism";
    String firstTable = "first_table";
    String secondTable = "second_table";
    boolean connectorRegistrationAttempted = false;

    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
      try {
        session.execute(
            "CREATE KEYSPACE "
                + keyspace
                + " WITH replication = {'class': 'NetworkTopologyStrategy', "
                + "'replication_factor': 1} AND tablets = {'initial': 8}");
        session.execute(
            "CREATE TABLE "
                + keyspace
                + "."
                + firstTable
                + " (id int PRIMARY KEY, value text) WITH cdc = {'enabled': true}");
        Thread.sleep(10);
        session.execute(
            "CREATE TABLE "
                + keyspace
                + "."
                + secondTable
                + " (id int PRIMARY KEY, value text) WITH cdc = {'enabled': true}");
      } catch (InvalidQueryException e) {
        abort("ScyllaDB version does not support CDC on tablet keyspaces: " + e.getMessage());
      }

      Date firstGeneration = generationTimestamp(session, keyspace, firstTable);
      Date secondGeneration = generationTimestamp(session, keyspace, secondTable);
      assertNotEquals(
          firstGeneration, secondGeneration, "Tables must exercise different generations");

      session.execute(
          "INSERT INTO " + keyspace + "." + firstTable + " (id, value) VALUES (1, 'one')");
      session.execute(
          "INSERT INTO " + keyspace + "." + secondTable + " (id, value) VALUES (2, 'two')");

      Properties configuration = KafkaConnectUtils.createCommonConnectorProperties();
      configuration.put("name", connectorName);
      configuration.put("topic.prefix", connectorName);
      configuration.put(
          "scylla.table.names", keyspace + "." + firstTable + "," + keyspace + "." + secondTable);
      configuration.put("tasks.max", "1");
      connectorRegistrationAttempted = true;
      KafkaConnectUtils.registerConnector(configuration, connectorName);

      awaitRunningTasks(connectorName, 1, Duration.ofSeconds(30));

      Set<String> expectedTopics =
          Set.of(
              connectorName + "." + keyspace + "." + firstTable,
              connectorName + "." + keyspace + "." + secondTable);
      consumer.subscribe(List.copyOf(expectedTopics));
      Set<String> consumedTopics = new HashSet<>();
      long deadline = System.currentTimeMillis() + Duration.ofSeconds(65).toMillis();
      while (!consumedTopics.containsAll(expectedTopics) && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofSeconds(5)).forEach(record -> consumedTopics.add(record.topic()));
      }
      assertEquals(expectedTopics, consumedTopics);
    } finally {
      if (connectorRegistrationAttempted) {
        KafkaConnectUtils.removeConnector(connectorName);
      }
      try (Cluster cluster =
              Cluster.builder()
                  .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                  .withPort(scyllaDBContainer.getMappedPort(9042))
                  .build();
          Session session = cluster.connect()) {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
      }
    }
  }

  private static Date generationTimestamp(Session session, String keyspace, String table) {
    Row row =
        session
            .execute(
                "SELECT timestamp FROM system.cdc_timestamps WHERE keyspace_name='"
                    + keyspace
                    + "' AND table_name='"
                    + table
                    + "' LIMIT 1")
            .one();
    assertNotNull(row, "Missing tablet CDC generation for " + table);
    return row.getTimestamp("timestamp");
  }

  private static void awaitRunningTasks(
      String connectorName, int expectedTaskCount, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    String lastStatus = null;
    while (System.currentTimeMillis() < deadline) {
      lastStatus = KafkaConnectUtils.getConnectorStatus(connectorName);
      if (lastStatus != null) {
        JsonNode tasks = OBJECT_MAPPER.readTree(lastStatus).path("tasks");
        if (tasks.size() == expectedTaskCount) {
          boolean allRunning = true;
          for (JsonNode task : tasks) {
            allRunning &= "RUNNING".equals(task.path("state").asText());
          }
          if (allRunning) {
            return;
          }
        }
      }
      Thread.sleep(500);
    }
    fail("Expected " + expectedTaskCount + " running tasks; last status: " + lastStatus);
  }
}

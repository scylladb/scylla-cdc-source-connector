package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import com.scylladb.cdc.model.TableName;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for CdcTableOptionsValidator. These tests verify that the validator correctly
 * detects when table CDC options (preimage/postimage) are missing or misconfigured.
 */
public class CdcTableOptionsValidatorIT extends AbstractContainerBaseIT {
  private static Session session;
  private static Cluster cluster;

  private static final String KEYSPACE = "cdc_options_test";
  private static final String TABLE_NO_PREIMAGE = "table_no_preimage";
  private static final String TABLE_WITH_PREIMAGE = "table_with_preimage";
  private static final String TABLE_WITH_POSTIMAGE = "table_with_postimage";
  private static final String TABLE_WITH_BOTH = "table_with_both";
  private static final String TABLE_NO_CDC = "table_no_cdc";

  @BeforeAll
  public static void setupClass() {
    cluster =
        Cluster.builder()
            .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
            .withPort(scyllaDBContainer.getMappedPort(9042))
            .build();
    session = cluster.connect();

    // Create keyspace
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + KEYSPACE
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

    // Create table with CDC but no preimage/postimage
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + KEYSPACE
            + "."
            + TABLE_NO_PREIMAGE
            + " (id int PRIMARY KEY, name text) WITH cdc = {'enabled': true}");

    // Create table with preimage
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + KEYSPACE
            + "."
            + TABLE_WITH_PREIMAGE
            + " (id int PRIMARY KEY, name text) WITH cdc = {'enabled': true, 'preimage': true}");

    // Create table with postimage
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + KEYSPACE
            + "."
            + TABLE_WITH_POSTIMAGE
            + " (id int PRIMARY KEY, name text) WITH cdc = {'enabled': true, 'postimage': true}");

    // Create table with both preimage and postimage
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + KEYSPACE
            + "."
            + TABLE_WITH_BOTH
            + " (id int PRIMARY KEY, name text) WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true}");

    // Create table without CDC
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + KEYSPACE
            + "."
            + TABLE_NO_CDC
            + " (id int PRIMARY KEY, name text)");
  }

  @AfterAll
  public static void cleanupClass() {
    if (session != null) {
      session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void testValidationPassesWithNoneMode() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.NONE, CdcIncludeMode.NONE);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_NO_PREIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertFalse(result.hasErrors(), "Should not have errors when mode is NONE");
    assertFalse(result.hasMissingTables(), "Should not have missing tables");
  }

  @Test
  void testValidationFailsWhenPreimageRequired() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.NONE);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_NO_PREIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertTrue(result.hasErrors(), "Should have errors when preimage is required but not enabled");
    assertEquals(1, result.getErrors().size());
    assertTrue(
        result.getErrors().get(0).contains("preimage"),
        "Error should mention preimage: " + result.getErrors().get(0));
  }

  @Test
  void testValidationPassesWhenPreimageEnabled() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.NONE);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_WITH_PREIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertFalse(result.hasErrors(), "Should not have errors when preimage is enabled");
  }

  @Test
  void testValidationFailsWhenPostimageRequired() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.NONE, CdcIncludeMode.FULL);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_NO_PREIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertTrue(result.hasErrors(), "Should have errors when postimage is required but not enabled");
    assertEquals(1, result.getErrors().size());
    assertTrue(
        result.getErrors().get(0).contains("postimage"),
        "Error should mention postimage: " + result.getErrors().get(0));
  }

  @Test
  void testValidationPassesWhenPostimageEnabled() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.NONE, CdcIncludeMode.FULL);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_WITH_POSTIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertFalse(result.hasErrors(), "Should not have errors when postimage is enabled");
  }

  @Test
  void testValidationPassesWhenBothRequired() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.FULL);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_WITH_BOTH));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertFalse(
        result.hasErrors(), "Should not have errors when both preimage and postimage are enabled");
  }

  @Test
  void testValidationFailsWhenBothRequiredButOnlyPreimage() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.FULL);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_WITH_PREIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertTrue(result.hasErrors(), "Should have errors when postimage is required but not enabled");
    assertEquals(1, result.getErrors().size());
    assertTrue(result.getErrors().get(0).contains("postimage"), "Error should mention postimage");
  }

  @Test
  void testValidationReportsMissingTable() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.NONE);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, "nonexistent_table"));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertFalse(result.hasErrors(), "Missing tables should not cause errors");
    assertTrue(result.hasMissingTables(), "Should report missing table");
    assertEquals(1, result.getMissingTables().size());
    assertTrue(
        result.getMissingTables().get(0).contains("nonexistent_table"),
        "Should contain table name");
  }

  @Test
  void testValidationReportsMissingKeyspace() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.NONE);

    Set<TableName> tables = Set.of(new TableName("nonexistent_keyspace", "some_table"));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertFalse(result.hasErrors(), "Missing keyspace should not cause errors");
    assertTrue(result.hasMissingTables(), "Should report missing table");
  }

  @Test
  void testValidationFailsWhenCdcNotEnabled() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.NONE);

    Set<TableName> tables = Set.of(new TableName(KEYSPACE, TABLE_NO_CDC));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertTrue(result.hasErrors(), "Should have errors when CDC is not enabled");
    assertTrue(
        result.getErrors().get(0).contains("CDC enabled"),
        "Error should mention CDC is not enabled: " + result.getErrors().get(0));
  }

  @Test
  void testValidationWithMultipleTables() {
    CdcTableOptionsValidator validator =
        new CdcTableOptionsValidator(CdcIncludeMode.FULL, CdcIncludeMode.NONE);

    Set<TableName> tables =
        Set.of(
            new TableName(KEYSPACE, TABLE_WITH_PREIMAGE),
            new TableName(KEYSPACE, TABLE_NO_PREIMAGE));

    CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tables);

    assertTrue(result.hasErrors(), "Should have errors for table without preimage");
    assertEquals(1, result.getErrors().size(), "Should only have one error");
    assertTrue(
        result.getErrors().get(0).contains(TABLE_NO_PREIMAGE),
        "Error should mention the table without preimage");
  }
}

package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import org.junit.jupiter.api.Test;

/** Unit tests for CdcIncludeMode enum parsing. */
public class CdcTableOptionsValidatorTest {

  @Test
  void testCdcIncludeModeParseNone() {
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse("none"));
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse("NONE"));
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse("None"));
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse(" none "));
  }

  @Test
  void testCdcIncludeModeParseFull() {
    assertEquals(CdcIncludeMode.FULL, CdcIncludeMode.parse("full"));
    assertEquals(CdcIncludeMode.FULL, CdcIncludeMode.parse("FULL"));
    assertEquals(CdcIncludeMode.FULL, CdcIncludeMode.parse("Full"));
    assertEquals(CdcIncludeMode.FULL, CdcIncludeMode.parse(" full "));
  }

  @Test
  void testCdcIncludeModeParseOnlyUpdated() {
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse("only-updated"));
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse("ONLY-UPDATED"));
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse("Only-Updated"));
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse(" only-updated "));
  }

  @Test
  void testCdcIncludeModeParseInvalid() {
    // Invalid values should default to NONE
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse(null));
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse(""));
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse("invalid"));
    assertEquals(CdcIncludeMode.NONE, CdcIncludeMode.parse("partial"));
  }

  @Test
  void testCdcIncludeModeValues() {
    assertEquals("none", CdcIncludeMode.NONE.getValue());
    assertEquals("full", CdcIncludeMode.FULL.getValue());
    assertEquals("only-updated", CdcIncludeMode.ONLY_UPDATED.getValue());
  }

  @Test
  void testCdcIncludeModeRequiresImage() {
    assertFalse(CdcIncludeMode.NONE.requiresImage());
    assertTrue(CdcIncludeMode.FULL.requiresImage());
    assertTrue(CdcIncludeMode.ONLY_UPDATED.requiresImage());
  }

  @Test
  void testValidationResultBasicOperations() {
    CdcTableOptionsValidator.ValidationResult result =
        new CdcTableOptionsValidator.ValidationResult();

    // Initially empty
    assertFalse(result.hasErrors());
    assertFalse(result.hasMissingTables());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.getMissingTables().isEmpty());

    // Add an error
    result.addError("Test error");
    assertTrue(result.hasErrors());
    assertEquals(1, result.getErrors().size());
    assertEquals("Test error", result.getErrors().get(0));

    // Add a missing table
    result.addMissingTable("ks.table");
    assertTrue(result.hasMissingTables());
    assertEquals(1, result.getMissingTables().size());
    assertEquals("ks.table", result.getMissingTables().get(0));
  }

  @Test
  void testValidationResultErrorMessage() {
    CdcTableOptionsValidator.ValidationResult result =
        new CdcTableOptionsValidator.ValidationResult();

    result.addError("Error 1");
    result.addError("Error 2");

    String errorMessage = result.getErrorMessage();
    assertTrue(errorMessage.contains("Error 1"));
    assertTrue(errorMessage.contains("Error 2"));
    assertTrue(errorMessage.contains("; "));
  }
}

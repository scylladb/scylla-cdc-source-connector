package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for ScyllaVersion. */
public class ScyllaVersionTest {

  @Test
  void parse_validVersion() {
    ScyllaVersion version = ScyllaVersion.parse("2026.1.0");
    assertNotNull(version);
    assertEquals(2026, version.getMajor());
    assertEquals(1, version.getMinor());
    assertEquals(0, version.getPatch());
  }

  @Test
  void parse_versionWithSuffix() {
    ScyllaVersion version = ScyllaVersion.parse("5.4.2-0.20240101.abc123");
    assertNotNull(version);
    assertEquals(5, version.getMajor());
    assertEquals(4, version.getMinor());
    assertEquals(2, version.getPatch());
  }

  @Test
  void parse_nullInput() {
    assertNull(ScyllaVersion.parse(null));
  }

  @Test
  void parse_emptyInput() {
    assertNull(ScyllaVersion.parse(""));
  }

  @Test
  void parse_invalidInput() {
    assertNull(ScyllaVersion.parse("invalid"));
  }

  @Test
  void compareTo_equal() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 1, 0);
    ScyllaVersion v2 = new ScyllaVersion(2026, 1, 0);
    assertEquals(0, v1.compareTo(v2));
  }

  @Test
  void compareTo_majorDifference() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 1, 0);
    ScyllaVersion v2 = new ScyllaVersion(5, 4, 2);
    assertTrue(v1.compareTo(v2) > 0);
    assertTrue(v2.compareTo(v1) < 0);
  }

  @Test
  void compareTo_minorDifference() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 2, 0);
    ScyllaVersion v2 = new ScyllaVersion(2026, 1, 0);
    assertTrue(v1.compareTo(v2) > 0);
    assertTrue(v2.compareTo(v1) < 0);
  }

  @Test
  void compareTo_patchDifference() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 1, 1);
    ScyllaVersion v2 = new ScyllaVersion(2026, 1, 0);
    assertTrue(v1.compareTo(v2) > 0);
    assertTrue(v2.compareTo(v1) < 0);
  }

  @Test
  void isAtLeast_true() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 1, 0);
    ScyllaVersion v2 = new ScyllaVersion(2026, 1, 0);
    assertTrue(v1.isAtLeast(v2));

    ScyllaVersion v3 = new ScyllaVersion(2026, 2, 0);
    assertTrue(v3.isAtLeast(v1));
  }

  @Test
  void isAtLeast_false() {
    ScyllaVersion v1 = new ScyllaVersion(5, 4, 2);
    ScyllaVersion v2 = new ScyllaVersion(2026, 1, 0);
    assertFalse(v1.isAtLeast(v2));
  }

  @Test
  void supportsPartitionDeletePreimage_true() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 1, 0);
    assertTrue(v1.supportsPartitionDeletePreimage());

    ScyllaVersion v2 = new ScyllaVersion(2026, 2, 0);
    assertTrue(v2.supportsPartitionDeletePreimage());

    ScyllaVersion v3 = new ScyllaVersion(2027, 0, 0);
    assertTrue(v3.supportsPartitionDeletePreimage());
  }

  @Test
  void supportsPartitionDeletePreimage_false() {
    ScyllaVersion v1 = new ScyllaVersion(5, 4, 2);
    assertFalse(v1.supportsPartitionDeletePreimage());

    ScyllaVersion v2 = new ScyllaVersion(2025, 2, 0);
    assertFalse(v2.supportsPartitionDeletePreimage());

    ScyllaVersion v3 = new ScyllaVersion(2026, 0, 9);
    assertFalse(v3.supportsPartitionDeletePreimage());
  }

  @Test
  void toString_format() {
    ScyllaVersion version = new ScyllaVersion(2026, 1, 0);
    assertEquals("2026.1.0", version.toString());
  }

  @Test
  void equals_andHashCode() {
    ScyllaVersion v1 = new ScyllaVersion(2026, 1, 0);
    ScyllaVersion v2 = new ScyllaVersion(2026, 1, 0);
    ScyllaVersion v3 = new ScyllaVersion(5, 4, 2);

    assertEquals(v1, v2);
    assertEquals(v1.hashCode(), v2.hashCode());
    assertFalse(v1.equals(v3));
  }

  @Test
  void partitionDeletePreimageSupportConstant() {
    assertEquals(2026, ScyllaVersion.PARTITION_DELETE_PREIMAGE_SUPPORT.getMajor());
    assertEquals(1, ScyllaVersion.PARTITION_DELETE_PREIMAGE_SUPPORT.getMinor());
    assertEquals(0, ScyllaVersion.PARTITION_DELETE_PREIMAGE_SUPPORT.getPatch());
  }
}

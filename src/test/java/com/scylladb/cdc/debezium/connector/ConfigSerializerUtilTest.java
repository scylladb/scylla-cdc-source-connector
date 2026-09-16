package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConfigSerializerUtilTest {

  @Test
  void rejectsInvalidCoordinationGroupNumbersConsistently() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ConfigSerializerUtil.deserializeCoordinationGroup(
                    "@coordination;bmFtZXNwYWNl;invalid;0;a3M;dGFibGU;1"));

    assertEquals("Invalid serialized coordination group", exception.getMessage());
    assertInstanceOf(NumberFormatException.class, exception.getCause());
  }
}

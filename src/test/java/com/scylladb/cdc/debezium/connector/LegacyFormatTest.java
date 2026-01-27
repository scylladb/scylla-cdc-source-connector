package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcOutputFormat;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.RawChange;
import io.debezium.config.Configuration;
import io.debezium.schema.DatabaseSchema;
import java.util.Map;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for legacy format support.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>CdcOutputFormat enum parsing
 *   <li>Configuration validation for incompatible options
 *   <li>ScyllaSchemaLegacy instantiation
 *   <li>ScyllaChangesConsumerLegacy instantiation and behavior
 *   <li>Schema factory pattern (correct type selection based on config)
 * </ul>
 */
public class LegacyFormatTest {

  private static final String MINIMAL_CONFIG_CLUSTER = "127.0.0.1:9042";
  private static final String MINIMAL_CONFIG_TABLE = "ks.table";

  /** Creates a configuration builder with minimal required fields. */
  private Configuration.Builder createMinimalConfigBuilder() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", MINIMAL_CONFIG_CLUSTER)
        .with("scylla.table.names", MINIMAL_CONFIG_TABLE);
  }

  @Nested
  class CdcOutputFormatEnumTests {

    @Test
    void parse_returnsLegacy_forLegacyValue() {
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse("legacy"));
    }

    @Test
    void parse_returnsAdvanced_forAdvancedValue() {
      assertEquals(CdcOutputFormat.ADVANCED, CdcOutputFormat.parse("advanced"));
    }

    @Test
    void parse_isCaseInsensitive() {
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse("LEGACY"));
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse("Legacy"));
      assertEquals(CdcOutputFormat.ADVANCED, CdcOutputFormat.parse("ADVANCED"));
      assertEquals(CdcOutputFormat.ADVANCED, CdcOutputFormat.parse("Advanced"));
    }

    @Test
    void parse_trimWhitespace() {
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse("  legacy  "));
      assertEquals(CdcOutputFormat.ADVANCED, CdcOutputFormat.parse("  advanced  "));
    }

    @Test
    void parse_returnsLegacy_forNullValue() {
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse(null));
    }

    @Test
    void parse_returnsLegacy_forInvalidValue() {
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse("invalid"));
      assertEquals(CdcOutputFormat.LEGACY, CdcOutputFormat.parse(""));
    }

    @Test
    void getValue_returnsCorrectStrings() {
      assertEquals("legacy", CdcOutputFormat.LEGACY.getValue());
      assertEquals("advanced", CdcOutputFormat.ADVANCED.getValue());
    }
  }

  @Nested
  class ConfigurationTests {

    @Test
    void defaultOutputFormat_isLegacy() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      assertEquals(CdcOutputFormat.LEGACY, connectorConfig.getCdcOutputFormat());
    }

    @Test
    void outputFormat_canBeSetToAdvanced() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "advanced")
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      assertEquals(CdcOutputFormat.ADVANCED, connectorConfig.getCdcOutputFormat());
    }

    @Test
    void outputFormat_canBeSetToLegacy() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      assertEquals(CdcOutputFormat.LEGACY, connectorConfig.getCdcOutputFormat());
    }

    @Test
    void preimagesEnabled_defaultIsFalse() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      assertFalse(connectorConfig.getPreimagesEnabled());
    }

    @Test
    void preimagesEnabled_canBeSetToTrue_inLegacyMode() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with("experimental.preimages.enabled", true)
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      assertTrue(connectorConfig.getPreimagesEnabled());
      assertEquals(CdcOutputFormat.LEGACY, connectorConfig.getCdcOutputFormat());
    }

    @Test
    void configKeyConstant_isCorrect() {
      assertEquals("cdc.output.format", ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY);
    }

    @Test
    void configKeyConstant_matchesFieldName() {
      assertEquals(
          ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY,
          ScyllaConnectorConfig.CDC_OUTPUT_FORMAT.name());
    }
  }

  @Nested
  class ConfigValidationTests {

    @Test
    void validation_fails_whenPreimagesEnabled_withAdvancedFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "advanced")
              .with("experimental.preimages.enabled", true)
              .build();

      // Validate the configuration
      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      // Find the validation result for experimental.preimages.enabled
      ConfigValue preimagesValidation = validationResults.get("experimental.preimages.enabled");
      assertNotNull(preimagesValidation);
      assertFalse(
          preimagesValidation.errorMessages().isEmpty(),
          "Expected validation error for incompatible preimages + advanced format");
      assertTrue(
          preimagesValidation.errorMessages().stream()
              .anyMatch(msg -> msg.contains("not compatible with cdc.output.format=advanced")));
    }

    @Test
    void validation_passes_whenPreimagesEnabled_withLegacyFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with("experimental.preimages.enabled", true)
              .build();

      // Validate the configuration
      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      // Find the validation result for experimental.preimages.enabled
      ConfigValue preimagesValidation = validationResults.get("experimental.preimages.enabled");
      assertNotNull(preimagesValidation);
      assertTrue(
          preimagesValidation.errorMessages().isEmpty(),
          "Expected no validation errors for preimages + legacy format");
    }

    @Test
    void validation_passes_whenPreimagesDisabled_withAdvancedFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "advanced")
              .with("experimental.preimages.enabled", false)
              .build();

      // Validate the configuration
      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      // Find the validation result for experimental.preimages.enabled
      ConfigValue preimagesValidation = validationResults.get("experimental.preimages.enabled");
      assertNotNull(preimagesValidation);
      assertTrue(
          preimagesValidation.errorMessages().isEmpty(),
          "Expected no validation errors when preimages disabled");
    }

    @Test
    void validation_fails_whenCdcIncludeBeforeFull_withLegacyFormat() {
      // cdc.include.before must be at default (none) in legacy mode
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY, "full")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includeBeforeValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY);
      assertNotNull(includeBeforeValidation);
      assertFalse(
          includeBeforeValidation.errorMessages().isEmpty(),
          "Expected validation error for cdc.include.before=full in legacy mode");
      assertTrue(
          includeBeforeValidation.errorMessages().stream()
              .anyMatch(msg -> msg.contains("must be 'none'")));
    }

    @Test
    void validation_fails_whenCdcIncludeAfterFull_withLegacyFormat() {
      // cdc.include.after must be at default (none) in legacy mode
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY, "full")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includeAfterValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY);
      assertNotNull(includeAfterValidation);
      assertFalse(
          includeAfterValidation.errorMessages().isEmpty(),
          "Expected validation error for cdc.include.after=full in legacy mode");
      assertTrue(
          includeAfterValidation.errorMessages().stream()
              .anyMatch(msg -> msg.contains("must be 'none'")));
    }

    @Test
    void validation_fails_whenCdcIncludePkNonDefault_withLegacyFormat() {
      // cdc.include.primary-key.placement must be at default in legacy mode
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_PK_KEY, "kafka-key")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includePkValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_PK_KEY);
      assertNotNull(includePkValidation);
      assertFalse(
          includePkValidation.errorMessages().isEmpty(),
          "Expected validation error for non-default cdc.include.primary-key.placement in legacy mode");
      assertTrue(
          includePkValidation.errorMessages().stream()
              .anyMatch(msg -> msg.contains("must be at default")));
    }

    @Test
    void validation_passes_whenCdcIncludePkDefault_withLegacyFormat() {
      // cdc.include.primary-key.placement at default value should pass in legacy mode
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with(
                  ScyllaConnectorConfig.CDC_INCLUDE_PK_KEY,
                  "kafka-key,payload-after,payload-before")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includePkValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_PK_KEY);
      assertNotNull(includePkValidation);
      assertTrue(
          includePkValidation.errorMessages().isEmpty(),
          "Expected no validation errors for default cdc.include.primary-key.placement in legacy mode");
    }

    @Test
    void validation_passes_whenCdcIncludeBeforeNone_withLegacyFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY, "none")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includeBeforeValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY);
      assertNotNull(includeBeforeValidation);
      assertTrue(
          includeBeforeValidation.errorMessages().isEmpty(),
          "Expected no validation errors for cdc.include.before=none with legacy format");
    }

    @Test
    void validation_passes_whenCdcIncludeBeforeFull_withAdvancedFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "advanced")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY, "full")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includeBeforeValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY);
      assertNotNull(includeBeforeValidation);
      assertTrue(
          includeBeforeValidation.errorMessages().isEmpty(),
          "Expected no validation errors for cdc.include.before=full with advanced format");
    }

    @Test
    void validation_passes_whenCdcIncludeAfterFull_withAdvancedFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "advanced")
              .with(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY, "full")
              .build();

      Map<String, ConfigValue> validationResults =
          config.validate(ScyllaConnectorConfig.ALL_FIELDS);

      ConfigValue includeAfterValidation =
          validationResults.get(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY);
      assertNotNull(includeAfterValidation);
      assertTrue(
          includeAfterValidation.errorMessages().isEmpty(),
          "Expected no validation errors for cdc.include.after=full with advanced format");
    }
  }

  @Nested
  class SchemaFactoryTests {

    @Test
    void schemaType_isScyllaSchemaLegacy_whenLegacyFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      DatabaseSchema<CollectionId> schema = createSchemaForConfig(connectorConfig);

      assertTrue(schema instanceof ScyllaSchemaLegacy);
    }

    @Test
    void schemaType_isScyllaSchema_whenAdvancedFormat() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "advanced")
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      DatabaseSchema<CollectionId> schema = createSchemaForConfig(connectorConfig);

      assertTrue(schema instanceof ScyllaSchema);
      assertFalse(schema instanceof ScyllaSchemaLegacy);
    }

    @Test
    void schemaType_isScyllaSchemaLegacy_whenDefaultFormat() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

      DatabaseSchema<CollectionId> schema = createSchemaForConfig(connectorConfig);

      assertTrue(schema instanceof ScyllaSchemaLegacy);
    }

    /** Helper method that mimics the schema creation logic in ScyllaConnectorTask. */
    private DatabaseSchema<CollectionId> createSchemaForConfig(ScyllaConnectorConfig config) {
      org.apache.kafka.connect.data.Schema sourceSchema =
          config.getSourceInfoStructMaker().schema();
      if (config.getCdcOutputFormat() == CdcOutputFormat.LEGACY) {
        return new ScyllaSchemaLegacy(config, sourceSchema);
      } else {
        return new ScyllaSchema(config, sourceSchema);
      }
    }
  }

  @Nested
  class ScyllaSchemaLegacyTests {

    @Test
    void cellValueConstant_isCorrect() {
      assertEquals("value", ScyllaSchemaLegacy.CELL_VALUE);
    }

    @Test
    void constructor_doesNotThrow() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();

      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      assertNotNull(schema);
    }

    @Test
    void schemaFor_returnsNull_whenNoChangeSchemaRegistered() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      CollectionId collectionId =
          new CollectionId(new com.scylladb.cdc.model.TableName("ks", "table"));

      assertNull(schema.schemaFor(collectionId));
    }

    @Test
    void tableInformationComplete_returnsFalse() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      assertFalse(schema.tableInformationComplete());
    }

    @Test
    void isHistorized_returnsFalse() {
      Configuration config = createMinimalConfigBuilder().build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      assertFalse(schema.isHistorized());
    }
  }

  @Nested
  class ScyllaChangesConsumerLegacyTests {

    @Test
    void constructor_doesNotThrow() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      ScyllaChangesConsumerLegacy consumer =
          new ScyllaChangesConsumerLegacy(null, null, schema, null, connectorConfig);

      assertNotNull(consumer);
    }

    @Test
    void preimageMap_isNull_whenPreimagesDisabled() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with("experimental.preimages.enabled", false)
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      ScyllaChangesConsumerLegacy consumer =
          new ScyllaChangesConsumerLegacy(null, null, schema, null, connectorConfig);

      Map<TaskId, RawChange> lastPreImage = consumer.getPreImageMapForTesting();

      assertNull(lastPreImage);
    }

    @Test
    void preimageMap_isHashMap_whenPreimagesEnabled() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .with("experimental.preimages.enabled", true)
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      ScyllaChangesConsumerLegacy consumer =
          new ScyllaChangesConsumerLegacy(null, null, schema, null, connectorConfig);

      Map<TaskId, RawChange> lastPreImage = consumer.getPreImageMapForTesting();

      assertNotNull(lastPreImage);
      assertEquals("java.util.HashMap", lastPreImage.getClass().getName());
    }
  }

  @Nested
  class ScyllaChangeRecordEmitterLegacyTests {

    @Test
    void constructor_doesNotThrow() {
      Configuration config =
          createMinimalConfigBuilder()
              .with(ScyllaConnectorConfig.CDC_OUTPUT_FORMAT_KEY, "legacy")
              .build();
      ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
      org.apache.kafka.connect.data.Schema sourceSchema =
          connectorConfig.getSourceInfoStructMaker().schema();
      ScyllaSchemaLegacy schema = new ScyllaSchemaLegacy(connectorConfig, sourceSchema);

      // Constructor with null values for dependencies (just testing instantiation)
      ScyllaChangeRecordEmitterLegacy emitter =
          new ScyllaChangeRecordEmitterLegacy(
              null, null, null, null, schema, null, connectorConfig);

      assertNotNull(emitter);
    }
  }
}

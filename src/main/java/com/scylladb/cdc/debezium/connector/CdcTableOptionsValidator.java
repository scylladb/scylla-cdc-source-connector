package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import com.scylladb.cdc.model.TableName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Validates that Scylla table CDC options match the connector configuration requirements.
 *
 * <p>This validator checks:
 *
 * <ul>
 *   <li>If cdc.include.before=full or only-updated, the table must have 'preimage':true in CDC
 *       options
 *   <li>If cdc.include.after=full or only-updated, the table must have 'postimage':true in CDC
 *       options
 * </ul>
 */
public class CdcTableOptionsValidator {

  /** Result of table validation */
  public static class ValidationResult {
    private final List<String> errors;
    private final List<String> missingTables;

    public ValidationResult() {
      this.errors = new ArrayList<>();
      this.missingTables = new ArrayList<>();
    }

    public void addError(String error) {
      errors.add(error);
    }

    public void addMissingTable(String tableName) {
      missingTables.add(tableName);
    }

    public boolean hasErrors() {
      return !errors.isEmpty();
    }

    public boolean hasMissingTables() {
      return !missingTables.isEmpty();
    }

    public List<String> getErrors() {
      return errors;
    }

    public List<String> getMissingTables() {
      return missingTables;
    }

    public String getErrorMessage() {
      return String.join("; ", errors);
    }
  }

  private final CdcIncludeMode includeBefore;
  private final CdcIncludeMode includeAfter;

  public CdcTableOptionsValidator(CdcIncludeMode includeBefore, CdcIncludeMode includeAfter) {
    this.includeBefore = includeBefore;
    this.includeAfter = includeAfter;
  }

  /**
   * Validates that all tables have the required CDC options enabled.
   *
   * @param cluster the Scylla cluster connection
   * @param tableNames the set of table names to validate
   * @return ValidationResult containing any errors or missing tables
   */
  public ValidationResult validate(Cluster cluster, Set<TableName> tableNames) {
    ValidationResult result = new ValidationResult();

    for (TableName tableName : tableNames) {
      validateTable(cluster, tableName, result);
    }

    return result;
  }

  private void validateTable(Cluster cluster, TableName tableName, ValidationResult result) {
    String keyspaceName = tableName.keyspace;
    String table = tableName.name;

    KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspaceName);
    if (keyspaceMetadata == null) {
      result.addMissingTable(tableName.keyspace + "." + tableName.name);
      return;
    }

    TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
    if (tableMetadata == null) {
      result.addMissingTable(tableName.keyspace + "." + tableName.name);
      return;
    }

    // Get CDC options from table options
    Optional<Map<String, String>> cdcOptions = getCdcOptions(tableMetadata);

    if (cdcOptions.isEmpty()) {
      // Table doesn't have CDC enabled at all
      if (includeBefore.requiresImage() || includeAfter.requiresImage()) {
        result.addError(
            String.format(
                "Table '%s.%s' does not have CDC enabled. "
                    + "Enable CDC with: ALTER TABLE %s.%s WITH cdc = {'enabled': true}",
                keyspaceName, table, keyspaceName, table));
      }
      return;
    }

    Map<String, String> options = cdcOptions.get();

    // Check preimage requirement
    if (includeBefore.requiresImage()) {
      String preimageValue = options.get("preimage");
      if (!"true".equalsIgnoreCase(preimageValue)) {
        result.addError(
            String.format(
                "Table '%s.%s' requires preimage for 'cdc.include.before=%s' but table has "
                    + "'preimage'=%s. Enable with: ALTER TABLE %s.%s WITH cdc = {'preimage': true}",
                keyspaceName, table, includeBefore.getValue(), preimageValue, keyspaceName, table));
      }
    }

    // Check postimage requirement
    if (includeAfter.requiresImage()) {
      String postimageValue = options.get("postimage");
      if (!"true".equalsIgnoreCase(postimageValue)) {
        result.addError(
            String.format(
                "Table '%s.%s' requires postimage for 'cdc.include.after=%s' but table has "
                    + "'postimage'=%s. Enable with: ALTER TABLE %s.%s WITH cdc = {'postimage': true}",
                keyspaceName, table, includeAfter.getValue(), postimageValue, keyspaceName, table));
      }
    }
  }

  /**
   * Extracts CDC options from table metadata using the Scylla driver's getScyllaCDCOptions method.
   *
   * <p>CDC options are returned as a map containing keys like 'enabled', 'preimage', 'postimage',
   * etc.
   *
   * @param tableMetadata the table metadata
   * @return Optional containing the CDC options map, or empty if CDC is not configured
   */
  private Optional<Map<String, String>> getCdcOptions(TableMetadata tableMetadata) {
    TableOptionsMetadata options = tableMetadata.getOptions();
    if (options == null) {
      return Optional.empty();
    }

    // Check if CDC is enabled using Scylla-specific method
    if (!options.isScyllaCDC()) {
      return Optional.empty();
    }

    // Get the Scylla CDC options map
    Map<String, String> cdcOptions = options.getScyllaCDCOptions();
    if (cdcOptions == null || cdcOptions.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(cdcOptions);
  }
}

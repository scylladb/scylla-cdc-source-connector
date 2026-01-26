package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class ConfigSerializerUtil {
  private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");
  private static final Pattern PORT_DELIMITER = Pattern.compile(":");
  private static final Pattern KEYSPACE_TABLE_DELIMITER = Pattern.compile("\\.");
  private static final Pattern UNQUOTED_NAME = Pattern.compile("[a-zA-Z_0-9]{1,48}");

  private static final String FIELD_DELIMITER = ";";
  private static final String STREAM_ID_DELIMITER = ",";

  /*
   * Serializes Task (Worker) config into String.
   *
   * Format:
   * "generationStart;vNodeIndex;keyspace;table;streamId1,streamId2,streamId3,..."
   */
  public static String serializeTaskConfig(TaskId taskId, SortedSet<StreamId> streamIds) {
    String generationStartEpoch =
        Long.toString(taskId.getGenerationId().getGenerationStart().toDate().getTime());
    String vNodeIndex = Integer.toString(taskId.getvNodeId().getIndex());
    String keyspace = taskId.getTable().keyspace;
    String table = taskId.getTable().name;
    String delimitedStreamIds =
        streamIds.stream()
            .map(StreamId::getValue)
            .map(Bytes::toHexString)
            .collect(Collectors.joining(STREAM_ID_DELIMITER));
    return String.join(
        FIELD_DELIMITER, generationStartEpoch, vNodeIndex, keyspace, table, delimitedStreamIds);
  }

  // TODO - introduce a new type for Pair<TaskId, SortedSet<StreamId>>?
  public static Pair<TaskId, SortedSet<StreamId>> deserializeTaskConfig(String serialized) {
    String[] fields = serialized.split(FIELD_DELIMITER);

    GenerationId generationId =
        new GenerationId(new Timestamp(new Date(Long.parseLong(fields[0]))));
    VNodeId vNodeId = new VNodeId(Integer.parseInt(fields[1]));
    TableName table = new TableName(fields[2], fields[3]);
    TaskId taskId = new TaskId(generationId, vNodeId, table);

    SortedSet<StreamId> streamIds =
        Arrays.stream(fields[4].split(STREAM_ID_DELIMITER))
            .map(Bytes::fromHexString)
            .map(StreamId::new)
            .collect(Collectors.toCollection(TreeSet<StreamId>::new));

    return Pair.of(taskId, streamIds);
  }

  /*
   * Deserializes a list of IP addresses, provided as a comma-separated list of pairs <IP>:<PORT>.
   */
  public static List<InetSocketAddress> deserializeClusterIpAddresses(String serialized) {
    String[] fields = COMMA_WITH_WHITESPACE.split(serialized);
    return Arrays.stream(fields)
        .map(ConfigSerializerUtil::deserializeClusterIpAddress)
        .collect(Collectors.toList());
  }

  public static int validateClusterIpAddresses(
      Configuration config, Field field, Field.ValidationOutput problems) {
    String clusterIpAddresses = config.getString(field);
    if (clusterIpAddresses == null) {
      problems.accept(field, clusterIpAddresses, "Host specification is required");
      return 1;
    }

    String[] splitClusterIpAddresses = COMMA_WITH_WHITESPACE.split(clusterIpAddresses);

    int count = 0;
    for (String clusterIpAddress : splitClusterIpAddresses) {
      String[] hostPort = PORT_DELIMITER.split(clusterIpAddress);

      if (hostPort.length != 2) {
        problems.accept(
            field,
            clusterIpAddress,
            "Expected host specification as a comma-separated list of pairs <IP>:<PORT>, but got invalid: "
                + clusterIpAddress);
        count++;
        continue;
      }

      String host = hostPort[0], port = hostPort[1];
      if (!StringUtils.isNumeric(port) || port.length() > 5) {
        // Maximum port length is 5 characters - 65535.
        problems.accept(
            field,
            clusterIpAddress,
            "Expected host specification as a comma-separated list of pairs <IP>:<PORT>, but got invalid port number: "
                + clusterIpAddress);
        count++;
        continue;
      }

      try {
        new InetSocketAddress(host, Integer.parseInt(port));
      } catch (Exception ex) {
        problems.accept(
            field,
            clusterIpAddress,
            "Expected host specification as a comma-separated list of pairs <IP>:<PORT>, but got invalid <IP>:<PORT> pair: "
                + clusterIpAddress);
        count++;
      }
    }

    return count;
  }

  private static InetSocketAddress deserializeClusterIpAddress(String serialized) {
    String[] hostPort = PORT_DELIMITER.split(serialized);

    return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
  }

  /*
   * Deserializes a list of table names, provided as a comma-separated list of <keyspace name>.<table name>.
   */
  public static Set<TableName> deserializeTableNames(String serialized) {
    String[] tables = COMMA_WITH_WHITESPACE.split(serialized);

    return Arrays.stream(tables)
        .map(
            t -> {
              String[] keyspaceAndName = KEYSPACE_TABLE_DELIMITER.split(t);

              return new TableName(keyspaceAndName[0], keyspaceAndName[1]);
            })
        .collect(Collectors.toSet());
  }

  public static int validateTableNames(
      Configuration config, Field field, Field.ValidationOutput problems) {
    String tableNames = config.getString(field);
    if (tableNames == null) {
      problems.accept(
          field, tableNames, "A table specification with at least one table is required");
      return 1;
    }

    String[] splitTableNames = COMMA_WITH_WHITESPACE.split(tableNames);

    int count = 0;
    for (String tableName : splitTableNames) {
      String[] keyspaceAndName = KEYSPACE_TABLE_DELIMITER.split(tableName);

      // Keyspace and table name limitations from here:
      // https://docs.scylladb.com/getting-started/ddl/#common-definitions
      //
      // TODO: Support quoted table names.

      if (keyspaceAndName.length != 2) {
        problems.accept(
            field,
            tableName,
            "Expected a comma-separated list of pairs <keyspace name>.<table name>, but got invalid: "
                + tableName);
        count++;
        continue;
      }

      String keyspace = keyspaceAndName[0], table = keyspaceAndName[1];
      if (!UNQUOTED_NAME.matcher(keyspace).matches()) {
        count++;
        problems.accept(
            field, tableName, "Got invalid keyspace name in table specification: " + keyspace);
      }
      if (!UNQUOTED_NAME.matcher(table).matches()) {
        count++;
        problems.accept(
            field, tableName, "Got invalid table name in table specification : " + table);
      }
    }
    return count;
  }

  /**
   * Validates the cdc.include.primary-key.placement configuration value.
   *
   * @param config the configuration
   * @param field the field being validated
   * @param problems output for validation problems
   * @return the number of validation errors found
   */
  public static int validateCdcIncludePk(
      Configuration config, Field field, Field.ValidationOutput problems) {
    String value = config.getString(field);
    if (value == null || value.trim().isEmpty()) {
      problems.accept(field, value, "cdc.include.primary-key.placement must be specified");
      return 1;
    }

    String[] locations = COMMA_WITH_WHITESPACE.split(value);
    int count = 0;

    for (String location : locations) {
      String trimmed = location.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      ScyllaConnectorConfig.CdcIncludePkLocation parsed =
          ScyllaConnectorConfig.CdcIncludePkLocation.parse(trimmed);
      if (parsed == null) {
        problems.accept(
            field,
            trimmed,
            "Invalid cdc.include.primary-key.placement location: '"
                + trimmed
                + "'. Valid values are: kafka-key, payload-after, payload-before, "
                + "payload-diff (reserved), payload-key, kafka-headers");
        count++;
      }
    }
    return count;
  }
}

package com.scylladb.cdc.debezium.connector;

import io.debezium.schema.TopicSelector;

import java.util.StringJoiner;

public class ScyllaTopicSelector {
    public static TopicSelector<CollectionId> defaultSelector(String prefix, String heartbeatPrefix) {
        return TopicSelector.defaultSelector(prefix, heartbeatPrefix, ".",
                ScyllaTopicSelector::getTopicName);
    }

    private static String getTopicName(CollectionId collectionId, String prefix, String delimiter) {
        StringJoiner sb = new StringJoiner(delimiter);

        if (prefix != null && prefix.trim().length() > 0) {
            String trimmedPrefix = prefix.trim();
            sb.add(trimmedPrefix);
        }

        sb.add(collectionId.getTableName().keyspace);
        sb.add(collectionId.getTableName().name);

        return sb.toString();
    }
}

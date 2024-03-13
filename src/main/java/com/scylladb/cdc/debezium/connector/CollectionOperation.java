package com.scylladb.cdc.debezium.connector;

public enum CollectionOperation {
    MODIFY, // Add or remove elements
    OVERWRITE, // Overwrite entire collection
}

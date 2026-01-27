# TODO: Issues Found in feat/non-frozen-collections Branch

This document lists problems and issues found during code review of the changes in the `feat/non-frozen-collections` branch.

## Critical Issues

### 1. ~~Missing list_col in Test Expectations~~ ✓ FIXED
**Location:** `src/test/java/com/scylladb/cdc/debezium/connector/ScyllaTypesNonFrozenCollectionsPlainConnectorIT.java:50-79`

~~The `expectedInsertWithValues()` method does not include `list_col` in the expected JSON output, only checking `set_col` and `map_col`. This means the test doesn't verify that list insertions work correctly.~~

**Fixed:** Added `"list_col": [10, 20, 30]` to `expectedInsertWithValues()` and `"list_col": [10]` to `expectedDelete()`. Lists are serialized as arrays of values (timeuuid keys are discarded).

---

### 2. README Documentation Mismatch
**Location:** `README.md`

The README was significantly simplified and now states:
- "No support for postimage, preimage needs to be enabled"
- References `experimental.preimages.enabled` configuration option

However, the actual code implements:
- Full postimage support via `cdc.include.after` configuration
- Configuration options `cdc.include.before`, `cdc.include.after`, and `cdc.include.primary-key.placement`

The documentation is inconsistent with the implementation and large sections of useful configuration documentation were removed.

**Fix:** Restore the preimage/postimage configuration documentation to match the actual implementation.
---

## Existing FIXME/TODO Comments

### 4. isFrozen() Workaround in ScyllaSchema
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ScyllaSchema.java:370-374`

```java
// FIXME: When isFrozen is fixed in scylla-cdc-java (PR #60),
// replace with just a call to isFrozen.
String deletedElementsColumnName = "cdc$deleted_elements_" + cdef.getColumnName();
return changeSchema.getAllColumnDefinitions().stream()
    .anyMatch(c -> c.getColumnName().equals(deletedElementsColumnName));
```

The connector uses a workaround to detect non-frozen collections by checking for CDC metadata columns instead of using the proper `isFrozen()` API.

**Status:** Blocked on scylla-cdc-java PR #60.

---

### 5. Unhandled InterruptedException
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ScyllaWorkerTransport.java:73`

```java
} catch (InterruptedException e) {
  // TODO - handle exception
}
```

InterruptedException is silently swallowed without proper handling.

**Fix:** Restore interrupt status and either rethrow or handle appropriately.

---

### 6. Generic Exception Handling in ScyllaConnector
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ScyllaConnector.java:163`

```java
} catch (Exception ex) {
  // TODO - catch specific exceptions, for example authentication error
```

All exceptions are caught generically instead of providing specific error messages for different failure types (authentication, connection, etc.).

---

### 7. ~~Database Name Returns Keyspace~~ ✓ FIXED
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/SourceInfo.java:92`

~~The semantic meaning may not align with Debezium's expectations.~~

**Fixed:** Replaced TODO comment with proper Javadoc explaining that this is intentional: Scylla/Cassandra uses "keyspace" instead of "database", so returning keyspace name satisfies Debezium's requirement.

---

### 8. Generation Info Not Persisted
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ScyllaMasterTransport.java:45`

```java
// TODO - persist generation info - do not start from first generation
return Optional.empty();
```

The connector always starts from the first generation rather than persisting and resuming from the last known generation.

---

### 9. No Support for Quoted Table Names
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ConfigSerializerUtil.java:174`

```java
// TODO: Support quoted table names.
```

Table names with special characters requiring quotes are not supported.

---

## Design Decisions

### 10. ✓ `only-updated` Mode is Placeholder for Future Implementation
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ScyllaChangeRecordEmitter.java`

The `only-updated` mode for `cdc.include.before` and `cdc.include.after` is intentionally NOT implemented. Using this mode throws `UnsupportedOperationException`.

**Current behavior:**
- For `before` data: Enable **preimage** on the table and use `cdc.include.before=full`
- For `after` data: Enable **postimage** on the table and use `cdc.include.after=full`
- Messages are formed directly from preimage/postimage without diff logic

**Rationale:** The connector does not apply any diff logic to the CDC data. Users who need before/after data must enable the corresponding image mode (preimage/postimage) on the table.

---

## Potential Logic Issues

### 11. List Delta Representation is Incomplete
**Location:** `src/main/java/com/scylladb/cdc/debezium/connector/ScyllaChangeRecordEmitter.java`

For non-frozen lists, the delta representation loses the timeuuid keys that Scylla uses internally. Deleted elements are represented as `null` values but without position information:

```java
if (deletedKeys != null) {
    for (int i = 0; i < deletedKeys.size(); i++) {
        delta.add(null);
    }
}
```

This means consumers cannot determine which list positions were modified.

**Consider:** Document this limitation clearly or provide element position information.

---

## Summary

| Priority | Count | Description |
|----------|-------|-------------|
| Critical | 3 (2 fixed) | Issues that affect correctness or production safety |
| FIXME/TODO | 6 (1 fixed) | Existing code comments marking incomplete work |
| Design | 1 (documented) | `only-updated` mode is placeholder for future |
| Logic | 1 | Potential logic issues requiring verification |
| Test Coverage | 2 (1 fixed) | Gaps in test coverage |
| Cleanup | 1 | Code quality improvements |

**Total Issues: 14 (4 fixed, 1 design decision documented)**

# TTTDB — Java Embedded DBMS Library

A lightweight **embedded database engine** written in **pure Java**, featuring on-disk storage, schema validation, B+Tree indexes, transactions, and a fluent SQL-like API.  
It’s designed for use as a standalone library that provides **SQL-style operations** (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) without an external database server.

---

## Features

- **Embedded & File-Based** — stores data directly on disk in paged table files.  
- **Transactional Safety** — automatic rollback on failed inserts, updates, or deletes.  
- **B+Tree Indexing** — fast lookups and range queries for indexed columns.  
- **Foreign Key & Check Constraints** — enforce relational integrity across tables.  
- **Fluent Query API** — type-safe SQL-like query building in Java.  
- **Page Cache System** — optimized for efficient on-disk access.  
- **Optional Unsafe Insert** — bypass transactions for maximum speed (no rollback).

---

## Project Structure

```
src/
├── main/java/com/database/tttdb/
│   ├── api/                 # Public API (DBMS, Schema, Query, UpdateFields, etc.)
│   ├── core/
│   │   ├── cache/           # Table and transaction cache management
│   │   ├── index/           # B+Tree and index management system
│   │   ├── manager/         # High-level CRUD and schema managers
│   │   ├── page/            # On-disk page and record structures
│   │   ├── parsing/         # Expression parser for computed updates
│   │   └── table/           # Internal table representation and constraints
│   ├── Database.java        # Core database container
│   ├── DBMS.java            # Entry point for all database operations
│   ├── FileIO*.java         # File I/O and async write threads
│   └── App.java             # Example entry point (optional)
└── test/java/com/database/tttdb/
    └── ...                  # Unit tests for all core components
```

---

## Installation

1. Clone and build the project:
   ```bash
   git clone https://github.com/YannisGatsios/TTTDB.git
   cd TTTDB
   mvn clean package
   ```

2. Add the resulting JAR from `target/` to your project’s classpath.

---

## Basic Usage

```java
// Define schema
Schema users = new Schema()
    .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
    .column("name").type(DataType.CHAR).size(20).unique().endColumn()
    .column("age").type(DataType.INT).defaultValue(18).endColumn();

DBMS dbms = new DBMS()
    .setPath("data/")
    .addDatabase("testdb", 100)
    .addTable(new DBMS.TableConfig("users", users));
    .start();

// Insert data
Row row = new Row(new String[]{"name", "age"}, new Object[]{"Alice", 30});
dbms.insert("users", row);

// Query data
List<Row> results = dbms.select(
    new Query.Select("*").from("users").where().column("age").isBigger(20).end().get()
);

dbms.close();
```

---

## Storage Model Overview

- Each **table** is persisted as one or more **pages** on disk.  
- Records within pages are stored **in arbitrary order** — no clustering.  
- Each **indexed column** maintains a **B+Tree** where:
  - `Key = record value`
  - `Value = BlockPointer(PageID, RowOffset)`
- On startup, the DBMS **rebuilds index pointers** into memory for fast access.  

---

## Transaction Behavior

| Operation | Transaction Used | Rollback on Failure | Notes |
|------------|------------------|---------------------|--------|
| `insert()` | ✅ Yes | ✅ Yes | Each insert batch is wrapped in a transaction. |
| `update()` | ✅ Yes | ✅ Yes | Rolls back on any schema or FK violation. |
| `delete()` | ✅ Yes | ✅ Yes | Ensures safe page compaction and index sync. |
| `insertUnsafe()` | ❌ No | ❌ No | Directly appends to disk — **no rollback or recovery**. Use carefully. |
| `select()` | ❌ No | ❌ No | Read-only operation, uses in-memory or cached pages. |

Transactions are implemented at the **database level** (see `Database.startTransaction()`, `commit()`, and `rollBack()`).  
Each operation handled by `EntryManager` (`insertEntries`, `updateEntry`, `deleteEntry`) automatically starts, commits, or rolls back its own transaction if needed.

---

## Internal Operation Summary

| Layer | Role | Key Classes |
|--------|------|-------------|
| **API Layer** | Provides user-facing query and schema definitions. | `DBMS`, `Query`, `Schema`, `UpdateFields` |
| **Manager Layer** | Implements CRUD logic and transaction boundaries. | `EntryManager`, `IndexManager`, `ForeignKeyManager` |
| **Index Layer** | B+Tree-based index for each indexed column. | `BPlusTree`, `Node`, `Pair` |
| **Storage Layer** | Handles table pages and record persistence. | `TablePage`, `Page`, `Entry` |
| **Cache Layer** | Maintains in-memory copies of frequently used pages. | `TableCache`, `TransactionCache` |

---

## Internal Guarantees

- All schema validations (type, size, NOT NULL, UNIQUE) occur **before writing to disk**.  
- Foreign keys are validated **before commit** using the `ForeignKeyManager`.  
- Pages are automatically compacted on deletion — the last record replaces the deleted one for O(1) removal.  
- Updates rebuild affected index entries atomically inside the transaction.  

---

## 🧪 Running Tests

All unit tests are located under `src/test/java/com/database/tttdb/` and can be run using:
```bash
mvn test
```

---

## Important Notes

- Only **table record pointers** are stored on disk — index tree structures are recreated at startup.  
- The **cache** ensures efficient access but may delay writes until commit.  
- **Unsafe inserts** do not start their own transaction. They write through the currently active cache (transaction cache if the user has started a transaction; otherwise the main cache). They therefore participate in any user-managed transaction, but skip the internal “create-and-rollback-on-error” transaction that safe batch inserts use. Use when you want full control over transaction boundaries (e.g., bulk import wrapped in your own startTransaction/commit).  

---
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

## Maven Dependency

Add this to your `pom.xml`:

```xml
<dependency>
  <groupId>com.database.tttdb</groupId>
  <artifactId>tttdb</artifactId>
  <version>1.0.0</version>
</dependency>
```

Then build and install the library locally:

```bash
mvn install
```
Maven will make it available from your local repository (`~/.m2/repository`).

---

## Basic Usage

The example below defines two related tables (`users` and `posts`) with a check constraint, a foreign key, and demonstrates insert, update, delete, and select operations:

```java
Schema users = new Schema()
    .column("username").type(DataType.CHAR).size(15).primaryKey().endColumn()
    .column("age").type(DataType.INT).endColumn()
    .check("age_check")
        .open().column("age").isBiggerOrEqual(18).end()
        .AND().column("age").isSmaller(130).end()
    .endCheck();

// === Define child table with FOREIGN KEY constraint ===
Schema posts = new Schema()
    .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
    .column("username").type(DataType.CHAR).size(15).endColumn()
    .column("content").type(DataType.CHAR).size(100).defaultValue("Empty").endColumn()
    .foreignKey("fk_user_post")
        .column("username")
        .reference().table("users").column("username").end()
        .onDelete(ForeignKeyAction.CASCADE)
    .endForeignKey();

// === Initialize database and tables ===
DBMS db = new DBMS()
    .setPath("data/")
    .addDatabase("social_db", 0)// 0 is the cache capacity
    .addTable(new DBMS.TableConfig("users", users))
    .addTable(new DBMS.TableConfig("posts", posts))
    .start();

// === Insert sample users ===
Row user1 = new Row("username,age")
.set("username", "Alice")
.set("age", 25);
db.insert("users", user1);

Row user2 = new Row("username,age")
.set("username", "Bob")
.set("age", 30);
db.insert("users", user2);

// === Insert posts ===
Row post1 = new Row("username,content")
.set("username", "Alice")
.set("content", "Hello World!");
db.insert("posts", post1);

Row post2 = new Row("username,content")
.set("username", "Bob")
.set("content", "Java Rocks!");
db.insert("posts", post2);

// === Update user data ===
Update update = new Update("users")
    .set()
        .selectColumn("age").set(26)
    .endUpdate()
    .where().column("username").isEqual("Alice").end()
    .endUpdateClause();
db.update(update);

// === Delete user (cascade removes related posts) ===
Delete delete = new Delete()
    .from("users")
    .where().column("username").isEqual("Bob").end()
    .endDeleteClause();
db.delete(delete);

// === Select remaining data ===
List<Row> results = db.select(
    new Query.Select("username,age").from("users").ASC("username")
);
List<Row> postResults = db.select(
    new Query.Select("username").from("posts").ASC("username")
);
for (Row r : results)
    System.out.println(r);
for (Row r : postResults) 
    System.out.println(r);

db.close();
```
### Expected Output
```
{username=Alice, age=26}
{username=Alice}
```

After execution, all database and index files are written to the configured storage path (`data/`):
```pgsql
data/
├── social_db.posts.id.index
├── social_db.posts.table
├── social_db.users.table
├── social_db.users.username.index
└── tttdb.log.0
```

Each `.index` file corresponds to an indexed column in the schema, while `.table` files store row data.  
The log file (`tttdb.log.0`) contains detailed runtime operations such as transactions, cache commits, and table management.

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
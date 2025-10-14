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
    .addDatabase("social_db", 0) // 0 = cache capacity
    .addTable("users", users)
    .addTable("posts", posts)
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
db.update("users")
    .set()
        .selectColumn("age").set(26)
    .endUpdate()
    .where().column("username").isEqual("Alice").end()
    .endUpdateClause()
    .execute();

// === Delete user (cascade removes related posts) ===
db.delete()
    .from("users")
    .where().column("username").isEqual("Bob").end()
    .endDeleteClause()
    .execute();

// === Select remaining data ===
List<Row> results = db.select("username,age")
    .from("users")
    .ASC("username")
    .fetch();

List<Row> postResults = db.select("username")
    .from("posts")
    .ASC("username")
    .fetch();

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

More usage examples can be found in `src/test/java/com/database/tttdb/AppTest.java`

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
- **Threading Model:** All API methods are expected to be called from a single application thread.  
  Each `Database` instance runs its own dedicated file I/O thread for persistence.  
  The `DBMS` and API layers perform **no synchronization or concurrency control**.  
  This design guarantees simplicity and consistency for embedded, single-threaded workloads.  

---

## Cache System Overview

The caching layer is the core performance component of TTTDB.  
Each `Database` instance owns one **Main Cache** and creates temporary **Transaction Caches** when transactions start.

### 1. Main Cache
- Implemented by the `Cache` class.
- Backed by a **LinkedHashMap** used as an **LRU (Least Recently Used)** cache.
- Pages (`TablePage` and `IndexPage`) are stored in memory until the cache reaches its capacity.
- When the cache exceeds capacity, the **eldest page is automatically written to disk** using the associated `FileIOThread`.

#### Write Policy
- **Write-back** caching: modified pages (“dirty pages”) stay in memory until evicted or committed.
- On `commit()`, all cached pages are written to disk in page-ID order.
- After commit, deleted or unused pages are truncated from the end of each table and index file to reclaim space.

#### Deletion and Truncation
- Each table and index tracks a set of deleted pages.
- When deleted pages exceed 10% of cache capacity, the cache triggers file truncation:
  - The tail of the file is physically removed.
  - Page counters and sets are updated to reflect the smaller file.

#### Rollback
- `rollback(reason)` clears the cache and reverts all tables and indexes to their last committed on-disk state.
- Any unflushed dirty pages are discarded.

---

### 2. Transaction Cache
- Implemented by the `TransactionCache` subclass.
- Created when `Database.startTransaction(name)` is called.
- Holds modified pages privately until commit or rollback.
- **Read-through behavior:** on cache miss, loads pages from the parent (usually the main cache).
- **Write-isolation:** writes never go directly to disk; they only merge into the parent cache on `commit()`.

#### Commit Behavior
1. Merges all modified pages into its parent cache.
2. If the parent is another `TransactionCache`, no disk I/O occurs yet.
3. If the parent is the main cache, all tables and indexes are committed to disk.

#### Rollback Behavior
- Simply discards all transaction-local pages.
- Restores the parent cache as the active one.

---

### 3. Internal Usage
- `Table` objects interact exclusively with the active cache (`Database.getCache()`):
  - Page retrieval: `getTablePage(PageKey)`
  - Page write/update: `put(PageKey, Page)`
  - Page removal/truncation after deletions
- `EntryManager` operations (insert, update, delete, select) use the cache indirectly through the `Table` layer.
- `FileIO` handles actual disk I/O asynchronously via the per-database `FileIOThread`.

This design provides:
- Fast in-memory access for active pages.
- Deferred writes for reduced disk I/O.
- Transactional isolation without explicit locks.
- Deterministic durability on commit.

---

## File I/O and Page Storage System

Each `Database` instance owns a single background thread for all disk I/O, encapsulated by `FileIOThread`.
All `Page` reads, writes, and truncations are enqueued as asynchronous tasks through `FileIO`.

## FileIOThread

- Dedicated per-database worker thread.
- Consumes a `BlockingQueue<Runnable>` of I/O tasks.
- Shuts down cleanly via a poison pill mechanism to ensure all pending writes complete.
- No synchronization or parallel file access is required—the single thread guarantees serial disk writes.

## FileIO

- High-level façade for page operations.
- Provides asynchronous methods:
  - `writePage()` — writes one page buffer.
  - `writePages()` — batch-writes grouped by file path.
  - `readPage()` — returns a `FutureTask<byte[]>` that resolves to the page bytes.
  - `truncateFile()` — shrinks table or index files after page deletions.
- Uses `RandomAccessFile` and `FileChannel` for direct positional writes and reads.
- Enforces page-size validation (`multiple of 4096 bytes`).
- Groups batched writes by table or index path for minimal file handle churn.
## Page System
Pages are the fixed-size storage units used for both table data and index blocks.
| type | class | Description |
|------------|------------------|---------------------|
|TablePage | `TablePage` | Stores serialized `Entry` objects representing table rows.
|IndexPage | `IndexPage` | Stores B+Tree index entries `(key, pointer)` linking to table rows.
Each page:
- Occupies exactly one 4096-byte block (or multiple thereof).
- Contains a header (`pageID`, `numOfEntries`, `spaceInUse`) and serialized entries.
- Is tracked as dirty when modified; written only on commit or eviction.
- Knows its own offset: `pageID * pageSize`.

## B+Tree Engine
`BPlusTree<K,V>` implements the balanced search tree for indexing:
- Logarithmic insert, delete, search, and range scan.
- Leaf chaining for range queries.
- Optional uniqueness and nullability.
- Automatic splitting/merging and rebalancing.
- Duplicate-key storage for non-unique indexes.

Each modification updates:
- In-memory nodes.
- Corresponding `IndexPage` via `IndexPageManager`.
- Recorded Operation in the `IndexSnapshot` for `rollback`.
## Data Flow Summary
```sql
User Operation (INSERT/UPDATE/DELETE)
    ↓
Table 
    ↓
EntryManager (modifies TablePage)
    ↓
Cache
    ↓
FileIOThread via FileIO.writePage()
    ↓
OS filesystem write
```
## Durability
- Commits flush dirty pages via batched `FileIO.writePages()`.
- Rollbacks discard modified cache pages before write-out.
- Index/table pages remain block-aligned for recovery.
- Truncation removes unused pages after deletions.
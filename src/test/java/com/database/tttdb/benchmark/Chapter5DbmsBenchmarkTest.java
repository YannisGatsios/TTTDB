package com.database.tttdb.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.database.tttdb.api.DBMS;
import com.database.tttdb.api.Row;
import com.database.tttdb.api.Schema;
import com.database.tttdb.core.index.IndexInit.IndexType;
import com.database.tttdb.core.table.DataType;

/**
 * DBMS-level benchmark suite for Chapter 5.
 *
 * These tests measure the cost of the full TTTDB path: API, schema validation,
 * managers, index layer, table pages, cache, transactions, and file I/O.
 */
class Chapter5DbmsBenchmarkTest {

    private static final Path OUT = BenchmarkConfig.OUTPUT_DIR.resolve("dbms-level-results.csv");

    @TempDir
    Path tempDir;

    @Test
    void runDbmsLevelBenchmarks() throws Exception {
        if (!BenchmarkConfig.RUN_DBMS_BENCHMARKS) {
            return;
        }

        List<BenchmarkResult> results = new ArrayList<>();
        for (int warmup = 0; warmup < BenchmarkConfig.WARMUPS; warmup++) {
            int warmupSize = Math.max(100, Math.min(BenchmarkConfig.DBMS_DATASET_SIZES[0], 1000));
            for (IndexType type : IndexType.values()) {
                runAllDbmsWorkloads(type, warmupSize, -1, false, results);
            }
        }

        for (int repetition = 1; repetition <= BenchmarkConfig.REPETITIONS; repetition++) {
            for (int size : BenchmarkConfig.DBMS_DATASET_SIZES) {
                for (IndexType type : IndexType.values()) {
                    runAllDbmsWorkloads(type, size, repetition, true, results);
                }
            }
        }

        BenchmarkCsvWriter.appendAll(OUT, results);
    }

    private void runAllDbmsWorkloads(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        safeBatchInsert(type, size, repetition, record, results, false);
        safeBatchInsert(type, size, repetition, record, results, true);
        equalitySearchByPrimaryKey(type, size, repetition, record, results);
        duplicateEqualitySearchBySecondaryIndex(type, size, repetition, record, results);
        rangeSearchBySecondaryIndex(type, size, repetition, record, results);
        deleteByPrimaryKey(type, size, repetition, record, results);
        memoryAfterDbmsBuild(type, size, repetition, record, results);
    }

    private void safeBatchInsert(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results,
        boolean randomOrder
    ) throws Exception {
        Path path = benchmarkPath(type, size, repetition, randomOrder ? "insert_random" : "insert_sequential");
        List<Row> rows = rows(size, randomOrder);
        DBMS db = null;
        try {
            db = createStartedDb(type, path, randomOrder ? "db_insert_random" : "db_insert_sequential");
            DBMS dbRef = db;
            BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> dbRef.insert("users", rows));
            db.commit();
            String workload = randomOrder ? "dbms_safe_batch_insert_random" : "dbms_safe_batch_insert_sequential";
            add(record, results, type, workload, size, repetition, size, m,
                "DBMS-level safe batch insert through db.insert(table, List<Row>)");
            assertEquals(size, db.select("username,age").from("users").fetch().size());
        } finally {
            closeAndClean(db, path);
        }
    }

    private void equalitySearchByPrimaryKey(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        Path path = benchmarkPath(type, size, repetition, "search_pk");
        DBMS db = null;
        try {
            db = createPopulatedDb(type, path, "db_search_pk", size, false);
            List<Integer> lookups = BenchmarkData.lookupKeys(size, BenchmarkConfig.LOOKUPS, BenchmarkConfig.RANDOM_SEED + repetition + 7);
            DBMS dbRef = db;
            BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
                long checksum = 0;
                for (int key : lookups) {
                    List<Row> found = dbRef.select("username,age")
                        .from("users")
                        .where().column("username").isEqual(BenchmarkData.username(key)).end()
                        .endSelectClause()
                        .fetch();
                    checksum += found.size();
                }
                return checksum;
            });
            assertTrue(m.checksum() > 0);
            add(record, results, type, "dbms_equality_search_primary_key", size, repetition, lookups.size(), m,
                "SELECT by indexed primary key username");
        } finally {
            closeAndClean(db, path);
        }
    }

    private void duplicateEqualitySearchBySecondaryIndex(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        Path path = benchmarkPath(type, size, repetition, "search_duplicate_age");
        DBMS db = null;
        try {
            db = createPopulatedDb(type, path, "db_search_age", size, false);
            int operations = Math.max(1, Math.min(BenchmarkConfig.LOOKUPS, size));
            DBMS dbRef = db;
            BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
                long checksum = 0;
                for (int i = 0; i < operations; i++) {
                    int age = 18 + (i % 63);
                    List<Row> found = dbRef.select("username,age")
                        .from("users")
                        .where().column("age").isEqual(age).end()
                        .endSelectClause()
                        .fetch();
                    checksum += found.size();
                }
                return checksum;
            });
            assertTrue(m.checksum() > 0);
            add(record, results, type, "dbms_duplicate_equality_search_secondary_index", size, repetition, operations, m,
                "SELECT by indexed non-unique age column");
        } finally {
            closeAndClean(db, path);
        }
    }

    private void rangeSearchBySecondaryIndex(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        Path path = benchmarkPath(type, size, repetition, "range_age");
        DBMS db = null;
        try {
            db = createPopulatedDb(type, path, "db_range_age", size, false);
            int operations = Math.max(1, Math.min(BenchmarkConfig.RANGE_QUERIES, size));
            DBMS dbRef = db;
            BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
                long checksum = 0;
                for (int i = 0; i < operations; i++) {
                    int from = 18 + (i % 40);
                    int to = Math.min(80, from + 10);
                    List<Row> found = dbRef.select("username,age")
                        .from("users")
                        .where().column("age").isBiggerOrEqual(from).end()
                        .AND().column("age").isSmallerOrEqual(to).end()
                        .endSelectClause()
                        .fetch();
                    checksum += found.size();
                }
                return checksum;
            });
            assertTrue(m.checksum() > 0);
            add(record, results, type, "dbms_range_search_secondary_index", size, repetition, operations, m,
                type == IndexType.HASH_INDEX ? "Range WHERE on Hash Index uses fallback behavior" : "Range WHERE on indexed age column");
        } finally {
            closeAndClean(db, path);
        }
    }

    private void deleteByPrimaryKey(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        Path path = benchmarkPath(type, size, repetition, "delete_pk");
        DBMS db = null;
        try {
            db = createPopulatedDb(type, path, "db_delete_pk", size, false);
            int operations = Math.max(1, Math.min(size / 10, BenchmarkConfig.LOOKUPS));
            DBMS dbRef = db;
            BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
                long checksum = 0;
                for (int i = 0; i < operations; i++) {
                    int key = i * 10;
                    int deleted = dbRef.delete()
                        .from("users")
                        .where().column("username").isEqual(BenchmarkData.username(key)).end()
                        .endDeleteClause()
                        .execute();
                    checksum += deleted;
                }
                return checksum;
            });
            db.commit();
            assertEquals(operations, m.checksum());
            add(record, results, type, "dbms_delete_by_primary_key", size, repetition, operations, m,
                "DELETE by indexed primary key username");
        } finally {
            closeAndClean(db, path);
        }
    }

    private void memoryAfterDbmsBuild(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        Path path = benchmarkPath(type, size, repetition, "memory_dbms_build");
        BenchmarkRuntime.forceGcPause();
        long before = BenchmarkRuntime.usedMemoryBytes();
        long start = System.nanoTime();
        DBMS db = createPopulatedDb(type, path, "db_memory", size, false);
        long elapsed = System.nanoTime() - start;
        BenchmarkRuntime.keepAlive(db);
        System.gc();
        Thread.sleep(50);
        long after = BenchmarkRuntime.usedMemoryBytes();
        long memory = Math.max(0L, after - before);
        BenchmarkRuntime.Measurement m = new BenchmarkRuntime.Measurement(elapsed, memory, size);
        add(record, results, type, "dbms_memory_after_build", size, repetition, size, m,
            "approximate heap delta after building populated DBMS; includes DBMS/cache/pages/index structures");
        closeAndClean(db, path);
    }

    private DBMS createPopulatedDb(IndexType type, Path path, String databaseName, int size, boolean randomOrder) throws Exception {
        DBMS db = createStartedDb(type, path, databaseName);
        db.insert("users", rows(size, randomOrder));
        db.commit();
        return db;
    }

    private DBMS createStartedDb(IndexType type, Path path, String databaseName) throws Exception {
        Files.createDirectories(path);
        Schema schema = new Schema()
            .column("username").type(DataType.CHAR).size(20).primaryKey().endColumn()
            .column("age").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(30).endColumn();

        return new DBMS()
            .addDatabase(databaseName, BenchmarkConfig.CACHE_CAPACITY)
            .setPath(path.toString())
            .setIndexType(type)
            .addTable("users", schema)
            .start()
            .selectDatabase(databaseName);
    }

    private List<Row> rows(int size, boolean randomOrder) {
        List<Integer> keys = randomOrder
            ? BenchmarkData.randomKeys(size, BenchmarkConfig.RANDOM_SEED + size)
            : BenchmarkData.sequentialKeys(size);
        List<Row> rows = new ArrayList<>(size);
        for (int key : keys) {
            rows.add(new Row("username,age,message")
                .set("username", BenchmarkData.username(key))
                .set("age", BenchmarkData.ageFor(key))
                .set("message", "m" + key));
        }
        return rows;
    }

    private Path benchmarkPath(IndexType type, int size, int repetition, String workload) {
        return tempDir.resolve(type.name() + "_" + size + "_" + repetition + "_" + workload);
    }

    private void closeAndClean(DBMS db, Path path) throws Exception {
        if (db != null) {
            try {
                db.dropDatabase();
            } catch (Exception ignored) {
                // Best-effort cleanup; do not hide benchmark failures.
            }
            try {
                db.close();
            } catch (Exception ignored) {
                // Best-effort cleanup; do not hide benchmark failures.
            }
        }
        if (Files.exists(path)) {
            try (var walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception ignored) {
                        // Temporary directory will also be cleaned by JUnit.
                    }
                });
            }
        }
    }

    private static void add(
        boolean record,
        List<BenchmarkResult> results,
        IndexType type,
        String workload,
        int size,
        int repetition,
        int operations,
        BenchmarkRuntime.Measurement measurement,
        String notes
    ) {
        if (!record) return;
        results.add(new BenchmarkResult(
            "dbms-level",
            type.name(),
            workload,
            size,
            repetition,
            operations,
            measurement.elapsedNanos(),
            measurement.memoryBytes(),
            notes,
            Instant.now()
        ));
    }
}

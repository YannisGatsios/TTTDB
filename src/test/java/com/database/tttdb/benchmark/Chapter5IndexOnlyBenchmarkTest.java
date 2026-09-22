package com.database.tttdb.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.database.tttdb.core.index.Index;
import com.database.tttdb.core.index.IndexInit.IndexType;
import com.database.tttdb.core.index.Pair;

/**
 * Index-only benchmark suite for Chapter 5.
 *
 * It isolates the four index implementations from the rest of the DBMS and measures
 * their raw behavior under the same operations and datasets.
 */
class Chapter5IndexOnlyBenchmarkTest {

    private static final Path OUT = BenchmarkConfig.OUTPUT_DIR.resolve("index-only-results.csv");

    @Test
    void runIndexOnlyBenchmarks() throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        for (int warmup = 0; warmup < BenchmarkConfig.WARMUPS; warmup++) {
            for (int size : smallWarmupSizes()) {
                for (IndexType type : IndexType.values()) {
                    runAllIndexWorkloads(type, size, -1, false, results);
                }
            }
        }

        for (int repetition = 1; repetition <= BenchmarkConfig.REPETITIONS; repetition++) {
            for (int size : BenchmarkConfig.INDEX_DATASET_SIZES) {
                for (IndexType type : IndexType.values()) {
                    runAllIndexWorkloads(type, size, repetition, true, results);
                }
            }
        }
        BenchmarkCsvWriter.appendAll(OUT, results);
    }

    private static int[] smallWarmupSizes() {
        int first = BenchmarkConfig.INDEX_DATASET_SIZES[0];
        return new int[]{Math.max(100, Math.min(first, 1000))};
    }

    private void runAllIndexWorkloads(
        IndexType type,
        int size,
        int repetition,
        boolean record,
        List<BenchmarkResult> results
    ) throws Exception {
        sequentialInsert(type, size, repetition, record, results);
        randomInsert(type, size, repetition, record, results);
        equalitySearchHit(type, size, repetition, record, results);
        equalitySearchMiss(type, size, repetition, record, results);
        duplicateKeySearch(type, size, repetition, record, results);
        rangeSearch(type, size, repetition, record, results);
        deleteWorkload(type, size, repetition, record, results);
        mixedWorkload(type, size, repetition, record, results);
        memoryFootprint(type, size, repetition, record, results);
    }

    private void sequentialInsert(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        List<Integer> keys = BenchmarkData.sequentialKeys(size);
        Index<Integer, Integer> index = IndexBenchmarkFactory.create(type, true);
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int key : keys) {
                index.insert(key, key);
                checksum += key;
            }
            BenchmarkRuntime.keepAlive(index);
            return checksum + index.size();
        });
        assertEquals(size, index.size());
        add(record, results, type, "sequential_unique_insert", size, repetition, size, m, "unique keys inserted in ascending order");
    }

    private void randomInsert(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        List<Integer> keys = BenchmarkData.randomKeys(size, BenchmarkConfig.RANDOM_SEED + repetition + size);
        Index<Integer, Integer> index = IndexBenchmarkFactory.create(type, true);
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int key : keys) {
                index.insert(key, key);
                checksum += key;
            }
            BenchmarkRuntime.keepAlive(index);
            return checksum + index.size();
        });
        assertEquals(size, index.size());
        add(record, results, type, "random_unique_insert", size, repetition, size, m, "unique keys inserted in shuffled order");
    }

    private void equalitySearchHit(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        Index<Integer, Integer> index = filledUniqueIndex(type, size);
        List<Integer> lookups = BenchmarkData.lookupKeys(size, BenchmarkConfig.LOOKUPS, BenchmarkConfig.RANDOM_SEED + repetition);
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int key : lookups) {
                List<Pair<Integer, Integer>> found = index.search(key);
                checksum += found.size();
                if (!found.isEmpty()) checksum += found.get(0).value;
            }
            return checksum;
        });
        assertTrue(m.checksum() > 0, "hit workload must find records");
        add(record, results, type, "equality_search_hit", size, repetition, lookups.size(), m, "random existing-key equality lookups");
    }

    private void equalitySearchMiss(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        Index<Integer, Integer> index = filledUniqueIndex(type, size);
        List<Integer> lookups = BenchmarkData.missingLookupKeys(size, BenchmarkConfig.LOOKUPS);
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int key : lookups) checksum += index.search(key).size();
            return checksum;
        });
        assertEquals(0, m.checksum(), "miss workload should not find records");
        add(record, results, type, "equality_search_miss", size, repetition, lookups.size(), m, "random absent-key equality lookups");
    }

    private void duplicateKeySearch(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        Index<Integer, Integer> index = filledDuplicateAgeIndex(type, size);
        List<Integer> lookups = new ArrayList<>();
        for (int i = 0; i < Math.min(BenchmarkConfig.LOOKUPS, size); i++) {
            lookups.add(18 + (i % 63));
        }
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int age : lookups) checksum += index.search(age).size();
            return checksum;
        });
        assertTrue(m.checksum() > 0, "duplicate-key searches must find records");
        add(record, results, type, "duplicate_key_search", size, repetition, lookups.size(), m, "age-like duplicate keys in range 18..80");
    }

    private void rangeSearch(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        Index<Integer, Integer> index = filledUniqueIndex(type, size);
        List<int[]> ranges = BenchmarkData.rangeQueries(size, BenchmarkConfig.RANGE_QUERIES, BenchmarkConfig.RANDOM_SEED + repetition + 99);
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int[] range : ranges) checksum += index.rangeSearch(range[0], range[1]).size();
            return checksum;
        });
        assertTrue(m.checksum() > 0, "range workload must return at least some rows");
        String note = type == IndexType.HASH_INDEX
            ? "Hash Index rangeSearch is supported through full scan fallback"
            : "ordered range query workload";
        add(record, results, type, "range_search", size, repetition, ranges.size(), m, note);
    }

    private void deleteWorkload(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        Index<Integer, Integer> index = filledUniqueIndex(type, size);
        int deleteCount = Math.max(1, Math.min(size / 10, BenchmarkConfig.LOOKUPS));
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int i = 0; i < deleteCount; i++) {
                int key = i * 10;
                index.remove(key, key);
                checksum += key;
            }
            return checksum + index.size();
        });
        assertEquals(size - deleteCount, index.size());
        add(record, results, type, "delete_existing_keys", size, repetition, deleteCount, m, "removes every tenth key, capped by lookup count");
    }

    private void mixedWorkload(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        int initial = Math.max(1, size / 2);
        Index<Integer, Integer> index = filledUniqueIndex(type, initial);
        int operations = Math.max(1, Math.min(BenchmarkConfig.LOOKUPS, size));
        BenchmarkRuntime.Measurement m = BenchmarkRuntime.measure(() -> {
            long checksum = 0;
            for (int i = 0; i < operations; i++) {
                int mode = i % 10;
                if (mode < 5) {
                    int key = initial + i;
                    index.insert(key, key);
                    checksum += key;
                } else if (mode < 9) {
                    checksum += index.search(i % Math.max(1, initial)).size();
                } else {
                    int key = i % Math.max(1, initial);
                    index.remove(key, key);
                    checksum += key;
                }
            }
            return checksum + index.size();
        });
        assertTrue(index.size() > 0);
        add(record, results, type, "mixed_insert_search_delete", size, repetition, operations, m, "50% insert, 40% search, 10% delete pattern");
    }

    private void memoryFootprint(IndexType type, int size, int repetition, boolean record, List<BenchmarkResult> results) throws Exception {
        List<Integer> keys = BenchmarkData.sequentialKeys(size);
        Index<Integer, Integer> index = IndexBenchmarkFactory.create(type, true);
        BenchmarkRuntime.forceGcPause();
        long before = BenchmarkRuntime.usedMemoryBytes();
        long start = System.nanoTime();
        long checksum = 0;
        for (int key : keys) {
            index.insert(key, key);
            checksum += key;
        }
        long elapsed = System.nanoTime() - start;
        BenchmarkRuntime.keepAlive(index);
        System.gc();
        Thread.sleep(50);
        long after = BenchmarkRuntime.usedMemoryBytes();
        BenchmarkRuntime.SINK ^= checksum + index.size();
        long memory = Math.max(0L, after - before);
        assertEquals(size, index.size());
        BenchmarkRuntime.Measurement m = new BenchmarkRuntime.Measurement(elapsed, memory, checksum);
        add(record, results, type, "memory_unique_index_after_insert", size, repetition, size, m, "approximate JVM heap delta after building unique index");
    }

    private Index<Integer, Integer> filledUniqueIndex(IndexType type, int size) {
        Index<Integer, Integer> index = IndexBenchmarkFactory.create(type, true);
        for (int i = 0; i < size; i++) index.insert(i, i);
        BenchmarkRuntime.keepAlive(index);
        return index;
    }

    private Index<Integer, Integer> filledDuplicateAgeIndex(IndexType type, int size) {
        Index<Integer, Integer> index = IndexBenchmarkFactory.create(type, false);
        for (int i = 0; i < size; i++) index.insert(18 + (i % 63), i);
        BenchmarkRuntime.keepAlive(index);
        return index;
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
            "index-only",
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

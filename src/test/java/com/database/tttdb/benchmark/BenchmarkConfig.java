package com.database.tttdb.benchmark;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Central configuration for Chapter 5 benchmarks.
 *
 * Override from Maven with -D properties, for example:
 * mvn -Dtttdb.benchmark.sizes=1000,10000,50000 -Dtttdb.benchmark.repetitions=5 test
 */
final class BenchmarkConfig {
    private BenchmarkConfig() {}

    static final int[] INDEX_DATASET_SIZES = parseIntList(
        "tttdb.benchmark.sizes",
        "1000,5000,10000,50000"
    );

    static final int[] DBMS_DATASET_SIZES = parseIntList(
        "tttdb.benchmark.dbms.sizes",
        "1000,5000,10000"
    );

    static final int REPETITIONS = intProperty("tttdb.benchmark.repetitions", 5);
    static final int WARMUPS = intProperty("tttdb.benchmark.warmups", 2);
    static final int LOOKUPS = intProperty("tttdb.benchmark.lookups", 1000);
    static final int RANGE_QUERIES = intProperty("tttdb.benchmark.rangeQueries", 200);
    static final int CACHE_CAPACITY = intProperty("tttdb.benchmark.cacheCapacity", 1000);
    static final long RANDOM_SEED = longProperty("tttdb.benchmark.seed", 42L);

    static final Path OUTPUT_DIR = Path.of(
        System.getProperty("tttdb.benchmark.outputDir", "target/chapter5-benchmarks")
    );

    static final boolean RUN_DBMS_BENCHMARKS = booleanProperty("tttdb.benchmark.runDbms", true);

    private static int intProperty(String name, int defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) return defaultValue;
        return Integer.parseInt(value.trim());
    }

    private static long longProperty(String name, long defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) return defaultValue;
        return Long.parseLong(value.trim());
    }

    private static boolean booleanProperty(String name, boolean defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) return defaultValue;
        return Boolean.parseBoolean(value.trim());
    }

    private static int[] parseIntList(String name, String defaultValue) {
        String raw = System.getProperty(name, defaultValue);
        String[] parts = raw.split(",");
        List<Integer> values = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) values.add(Integer.parseInt(trimmed));
        }
        if (values.isEmpty()) throw new IllegalArgumentException("No dataset sizes configured for " + name);
        int[] out = new int[values.size()];
        for (int i = 0; i < values.size(); i++) out[i] = values.get(i);
        return out;
    }
}

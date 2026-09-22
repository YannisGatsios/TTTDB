package com.database.tttdb.benchmark;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Writes a reproducibility report for Chapter 5. This is not a performance test;
 * it records the JVM/OS settings that should be mentioned in the thesis.
 */
class Chapter5EnvironmentReportTest {

    @Test
    void writeBenchmarkEnvironmentReport() throws IOException {
        Path out = BenchmarkConfig.OUTPUT_DIR.resolve("environment.csv");
        Files.createDirectories(out.getParent());

        Runtime runtime = Runtime.getRuntime();
        Map<String, String> values = new LinkedHashMap<>();
        values.put("timestamp", Instant.now().toString());
        values.put("os.name", System.getProperty("os.name"));
        values.put("os.version", System.getProperty("os.version"));
        values.put("os.arch", System.getProperty("os.arch"));
        values.put("java.version", System.getProperty("java.version"));
        values.put("java.vendor", System.getProperty("java.vendor"));
        values.put("jvm.name", System.getProperty("java.vm.name"));
        values.put("available.processors", Integer.toString(runtime.availableProcessors()));
        values.put("max.memory.bytes", Long.toString(runtime.maxMemory()));
        values.put("total.memory.bytes", Long.toString(runtime.totalMemory()));
        values.put("benchmark.index.sizes", join(BenchmarkConfig.INDEX_DATASET_SIZES));
        values.put("benchmark.dbms.sizes", join(BenchmarkConfig.DBMS_DATASET_SIZES));
        values.put("benchmark.repetitions", Integer.toString(BenchmarkConfig.REPETITIONS));
        values.put("benchmark.warmups", Integer.toString(BenchmarkConfig.WARMUPS));
        values.put("benchmark.lookups", Integer.toString(BenchmarkConfig.LOOKUPS));
        values.put("benchmark.rangeQueries", Integer.toString(BenchmarkConfig.RANGE_QUERIES));
        values.put("benchmark.cacheCapacity", Integer.toString(BenchmarkConfig.CACHE_CAPACITY));
        values.put("benchmark.seed", Long.toString(BenchmarkConfig.RANDOM_SEED));
        values.put("jvm.input.arguments", ManagementFactory.getRuntimeMXBean().getInputArguments().toString());

        StringBuilder sb = new StringBuilder("parameter,value\n");
        values.forEach((k, v) -> sb.append(csv(k)).append(',').append(csv(v)).append('\n'));
        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);
        assertTrue(Files.exists(out));
    }

    private static String join(int[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(';');
            sb.append(values[i]);
        }
        return sb.toString();
    }

    private static String csv(String value) {
        return "\"" + String.valueOf(value).replace("\"", "\"\"") + "\"";
    }
}

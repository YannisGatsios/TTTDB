package com.database.tttdb.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

final class BenchmarkCsvWriter {
    private BenchmarkCsvWriter() {}

    static void reset(Path file) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, header(), StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    static void append(Path file, BenchmarkResult result) throws IOException {
        Files.createDirectories(file.getParent());
        if (!Files.exists(file)) reset(file);
        Files.writeString(file, row(result), StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    static void appendAll(Path file, List<BenchmarkResult> results) throws IOException {
        reset(file);
        StringBuilder sb = new StringBuilder();
        for (BenchmarkResult result : results) sb.append(row(result));
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    private static String header() {
        return String.join(",",
            "timestamp",
            "benchmark_level",
            "index_type",
            "workload",
            "dataset_size",
            "repetition",
            "operations",
            "elapsed_ns",
            "elapsed_ms",
            "avg_ns_per_op",
            "ops_per_sec",
            "memory_bytes",
            "memory_mb",
            "notes"
        ) + "\n";
    }

    private static String row(BenchmarkResult r) {
        return String.join(",",
            csv(r.timestamp().toString()),
            csv(r.benchmarkLevel()),
            csv(r.indexType()),
            csv(r.workload()),
            Integer.toString(r.datasetSize()),
            Integer.toString(r.repetition()),
            Integer.toString(r.operations()),
            Long.toString(r.elapsedNanos()),
            Double.toString(r.elapsedMillis()),
            Double.toString(r.avgNanosPerOperation()),
            Double.toString(r.operationsPerSecond()),
            Long.toString(r.memoryBytes()),
            Double.toString(r.memoryMegabytes()),
            csv(r.notes())
        ) + "\n";
    }

    private static String csv(String value) {
        if (value == null) return "";
        String escaped = value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }
}

package com.database.tttdb.benchmark;

import java.time.Instant;

record BenchmarkResult(
    String benchmarkLevel,
    String indexType,
    String workload,
    int datasetSize,
    int repetition,
    int operations,
    long elapsedNanos,
    long memoryBytes,
    String notes,
    Instant timestamp
) {
    double elapsedMillis() {
        return elapsedNanos / 1_000_000.0;
    }

    double avgNanosPerOperation() {
        return operations <= 0 ? 0.0 : (double) elapsedNanos / operations;
    }

    double operationsPerSecond() {
        return elapsedNanos <= 0 ? 0.0 : operations / (elapsedNanos / 1_000_000_000.0);
    }

    double memoryMegabytes() {
        return memoryBytes / (1024.0 * 1024.0);
    }
}

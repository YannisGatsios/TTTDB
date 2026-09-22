package com.database.tttdb.benchmark;

import java.util.concurrent.TimeUnit;

final class BenchmarkRuntime {
    private BenchmarkRuntime() {}

    static volatile Object KEEP_ALIVE;
    static volatile long SINK;

    static long usedMemoryBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    static void forceGcPause() {
        KEEP_ALIVE = null;
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                TimeUnit.MILLISECONDS.sleep(40);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    static void keepAlive(Object object) {
        KEEP_ALIVE = object;
    }

    static long checksumOf(Object value) {
        return value == null ? 0 : value.hashCode();
    }

    @FunctionalInterface
    interface ThrowingLongSupplier {
        long getAsLong() throws Exception;
    }

    static Measurement measure(ThrowingLongSupplier operation) throws Exception {
        long start = System.nanoTime();
        long checksum = operation.getAsLong();
        long elapsed = System.nanoTime() - start;
        SINK ^= checksum;
        return new Measurement(elapsed, 0L, checksum);
    }

    static Measurement measureWithMemory(ThrowingLongSupplier operation, Object keepAlive) throws Exception {
        forceGcPause();
        long before = usedMemoryBytes();
        long start = System.nanoTime();
        long checksum = operation.getAsLong();
        long elapsed = System.nanoTime() - start;
        keepAlive(keepAlive);
        for (int i = 0; i < 2; i++) {
            System.gc();
            try {
                TimeUnit.MILLISECONDS.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        long after = usedMemoryBytes();
        long memory = Math.max(0L, after - before);
        SINK ^= checksum;
        return new Measurement(elapsed, memory, checksum);
    }

    record Measurement(long elapsedNanos, long memoryBytes, long checksum) {}
}

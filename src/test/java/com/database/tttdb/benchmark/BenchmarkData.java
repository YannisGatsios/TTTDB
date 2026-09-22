package com.database.tttdb.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

final class BenchmarkData {
    private BenchmarkData() {}

    static List<Integer> sequentialKeys(int n) {
        List<Integer> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) keys.add(i);
        return keys;
    }

    static List<Integer> randomKeys(int n, long seed) {
        List<Integer> keys = sequentialKeys(n);
        Collections.shuffle(keys, new Random(seed));
        return keys;
    }

    static List<Integer> lookupKeys(int n, int count, long seed) {
        Random random = new Random(seed);
        int actual = Math.max(1, Math.min(count, n));
        List<Integer> keys = new ArrayList<>(actual);
        for (int i = 0; i < actual; i++) keys.add(random.nextInt(n));
        return keys;
    }

    static List<Integer> missingLookupKeys(int n, int count) {
        int actual = Math.max(1, Math.min(count, n));
        List<Integer> keys = new ArrayList<>(actual);
        for (int i = 0; i < actual; i++) keys.add(n + i + 1);
        return keys;
    }

    static List<int[]> rangeQueries(int n, int count, long seed) {
        Random random = new Random(seed);
        int actual = Math.max(1, Math.min(count, Math.max(1, n)));
        int window = Math.max(10, n / 100);
        List<int[]> ranges = new ArrayList<>(actual);
        for (int i = 0; i < actual; i++) {
            int from = random.nextInt(Math.max(1, n));
            int to = Math.min(n - 1, from + window);
            if (from > to) from = to;
            ranges.add(new int[]{from, to});
        }
        return ranges;
    }

    static String username(int id) {
        return String.format("user_%06d", id);
    }

    static int ageFor(int id) {
        return 18 + (id % 63);
    }
}

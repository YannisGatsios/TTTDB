package com.database.tttdb.index;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.database.tttdb.core.index.Pair;
import com.database.tttdb.core.index.redBlackTreeIndex.RedBlackTreeIndex;

/**
 * Stress-oriented tests for RedBlackTreeIndex. These tests exercise rotations,
 * recoloring and delete fix-up through many insertions and removals.
 */
class RedBlackTreeIndexStressTest {

    @Test
    void manySequentialInsertsAndDeletesKeepSearchAndRangeCorrect() {
        RedBlackTreeIndex<Integer, Integer> index = new RedBlackTreeIndex<>();
        index.setUnique(true);

        for (int i = 1; i <= 1_000; i++) {
            index.insert(i, i * 10);
        }
        assertEquals(1_000, index.size());
        assertEquals(1_000, index.getMax());

        for (int i = 1; i <= 1_000; i += 2) {
            index.remove(i, i * 10);
        }
        assertEquals(500, index.size());

        for (int i = 1; i <= 1_000; i++) {
            if (i % 2 == 0) {
                assertEquals(i * 10, index.search(i).get(0).value);
            } else {
                assertTrue(index.search(i).isEmpty(), "odd key should have been removed: " + i);
            }
        }

        Set<Integer> rangeValues = index.rangeSearch(990, 1_000)
            .stream()
            .map((Pair<Integer, Integer> p) -> p.value)
            .collect(Collectors.toSet());

        assertEquals(Set.of(9900, 9920, 9940, 9960, 9980, 10000), rangeValues);
    }
}

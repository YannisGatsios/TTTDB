package com.database.tttdb.index;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import com.database.tttdb.core.index.Index;
import com.database.tttdb.core.index.Pair;
import com.database.tttdb.core.index.btree.BPlusTree;
import com.database.tttdb.core.index.hashmap.HashIndex;
import com.database.tttdb.core.index.redBlackTreeIndex.RedBlackTreeIndex;
import com.database.tttdb.core.index.skiplist.SkipListIndex;

/**
 * Contract tests shared by all index implementations.
 *
 * These tests intentionally avoid implementation-specific assumptions such as
 * physical tree shape or hash bucket ordering. Their purpose is to verify that
 * B+Tree, HashIndex, RedBlackTreeIndex and SkipListIndex can all be used through
 * the common Index<K,V> interface.
 */
class IndexImplementationContractTest {

    private record Impl(String name, Supplier<Index<Integer, String>> factory) {}

    private static List<Impl> implementations() {
        return List.of(
            new Impl("BPlusTree", () -> new BPlusTree<Integer, String>(4)),
            new Impl("HashIndex", HashIndex::new),
            new Impl("RedBlackTreeIndex", RedBlackTreeIndex::new),
            new Impl("SkipListIndex", SkipListIndex::new)
        );
    }

    @TestFactory
    Stream<DynamicTest> nonUniqueInsertSearchRemoveAndRangeWorkForEveryIndex() {
        return implementations().stream().map(impl -> dynamicTest(impl.name(), () -> {
            Index<Integer, String> index = impl.factory().get();
            index.setUnique(false);

            index.insert(10, "A");
            index.insert(10, "B");
            index.insert(20, "C");
            index.insert(30, "D");

            assertEquals(4, index.size(), "size should count stored key-value pairs");
            assertEquals(Set.of("A", "B"), values(index.search(10)));
            assertTrue(index.isKey(10));
            assertFalse(index.isKey(99));

            List<Pair<Integer, String>> range = index.rangeSearch(10, 20);
            assertEquals(Set.of("A", "B", "C"), values(range));

            index.remove(10, "A");
            assertEquals(3, index.size());
            assertEquals(Set.of("B"), values(index.search(10)));

            index.remove(10, "missing");
            index.remove(999, "missing");
            assertEquals(3, index.size(), "removing missing pairs must not change size");
        }));
    }

    @TestFactory
    Stream<DynamicTest> uniqueUpdateGetMaxAndClearWorkForEveryIndex() {
        return implementations().stream().map(impl -> dynamicTest(impl.name(), () -> {
            Index<Integer, String> index = impl.factory().get();
            index.setUnique(true);

            index.insert(5, "five");
            index.insert(2, "two");
            index.insert(9, "nine");

            index.update(5, "FIVE");
            assertEquals(List.of("FIVE"), new ArrayList<>(values(index.search(5))));
            assertEquals(9, index.getMax());

            assertEquals(Set.of("two", "FIVE", "nine"), values(index.rangeSearch(2, 9)));

            index.clear();
            assertEquals(0, index.size());
            assertTrue(index.search(5).isEmpty());
            assertFalse(index.isKey(5));
        }));
    }

    @TestFactory
    Stream<DynamicTest> nullableNonUniqueKeysWorkForEveryIndex() {
        return implementations().stream().map(impl -> dynamicTest(impl.name(), () -> {
            Index<Integer, String> index = impl.factory().get();
            index.setNullable(true);
            index.setUnique(false);

            index.insert(null, "NULL-1");
            index.insert(null, "NULL-2");
            index.insert(1, "A");

            assertEquals(Set.of("NULL-1", "NULL-2"), values(index.search(null)));
            assertTrue(index.isKey(null));

            Set<String> allValues = values(index.rangeSearch(null, null));
            assertTrue(allValues.contains("NULL-1"));
            assertTrue(allValues.contains("NULL-2"));
            assertTrue(allValues.contains("A"));
        }));
    }

    @TestFactory
    Stream<DynamicTest> rangeBoundsAreInclusiveForEveryIndex() {
        return implementations().stream().map(impl -> dynamicTest(impl.name(), () -> {
            Index<Integer, String> index = impl.factory().get();
            index.setUnique(false);

            index.insert(10, "A");
            index.insert(20, "B");
            index.insert(30, "C");
            index.insert(40, "D");

            assertEquals(Set.of("B", "C"), values(index.rangeSearch(20, 30)));
            assertEquals(Set.of("A", "B"), values(index.rangeSearch(null, 20)));
            assertEquals(Set.of("C", "D"), values(index.rangeSearch(30, null)));
        }));
    }

    @TestFactory
    Stream<DynamicTest> nonUniqueUpdateSpecificOldValueWorksForEveryIndex() {
        return implementations().stream().map(impl -> dynamicTest(impl.name(), () -> {
            Index<Integer, String> index = impl.factory().get();
            index.setUnique(false);

            index.insert(7, "old");
            index.insert(7, "keep");
            index.update(7, "new", "old");

            Set<String> values = values(index.search(7));
            assertTrue(values.contains("new"));
            assertTrue(values.contains("keep"));
            assertFalse(values.contains("old"));
        }));
    }

    private static <K, V> Set<V> values(List<Pair<K, V>> pairs) {
        return pairs.stream().map(p -> p.value).collect(Collectors.toCollection(HashSet::new));
    }
}

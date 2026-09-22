package com.database.tttdb.index;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import com.database.tttdb.core.index.IndexInit;
import com.database.tttdb.core.index.IndexInit.BlockPointer;
import com.database.tttdb.core.index.IndexInit.IndexType;
import com.database.tttdb.core.index.IndexInit.PointerPair;
import com.database.tttdb.core.index.Pair;

/**
 * Tests the adapter used by TTTDB to select the underlying index implementation.
 * The DBMS does not store raw values in indexes; it stores PointerPair values
 * that connect an indexed key with both table and index page positions.
 */
class IndexInitDelegationTest {

    @TestFactory
    Stream<DynamicTest> indexInitDelegatesCoreOperationsToEveryIndexType() {
        return Stream.of(IndexType.values()).map(type -> dynamicTest(type.name(), () -> {
            IndexInit<Integer> index = new IndexInit<>(type);
            index.setUnique(false);
            index.setNullable(true);
            index.setColumnIndex(1);

            PointerPair p1 = pair(3, 7, 0, 0);
            PointerPair p2 = pair(4, 1, 0, 1);
            PointerPair p3 = pair(8, 2, 0, 2);

            index.insert(25, p1);
            index.insert(25, p2);
            index.insert(30, p3);

            assertEquals(1, index.getColumnIndex());
            assertEquals(3, index.size());
            assertTrue(index.isKey(25));
            assertEquals(Set.of(p1, p2), values(index.search(25)));
            assertEquals(Set.of(p1, p2, p3), values(index.rangeSearch(25, 30)));

            PointerPair p2Updated = pair(9, 9, 0, 1);
            index.update(25, p2Updated, p2);
            assertEquals(Set.of(p1, p2Updated), values(index.search(25)));

            index.remove(25, p1);
            assertEquals(Set.of(p2Updated), values(index.search(25)));
        }));
    }

    private static PointerPair pair(int tablePage, int tableOffset, int indexPage, int indexOffset) {
        return new PointerPair(
            new BlockPointer(tablePage, (short) tableOffset),
            new BlockPointer(indexPage, (short) indexOffset)
        );
    }

    private static Set<PointerPair> values(List<Pair<Integer, PointerPair>> pairs) {
        return pairs.stream().map(p -> p.value).collect(Collectors.toSet());
    }
}

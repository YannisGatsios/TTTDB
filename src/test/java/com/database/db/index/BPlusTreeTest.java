package com.database.db.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

class BPlusTreeTest {
    private static final int ORDER = 3;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random(42);  // Seed for reproducibility
    }

    // ================ TEST HELPERS ================
    private <K extends Comparable<K>, V> void validateTreeStructure(BPlusTree<K, V> tree) {
        if (tree.getRoot() == null) return;
        
        Queue<Node<K, V>> queue = new LinkedList<>();
        queue.add(tree.getRoot());
        int level = 0;

        while (!queue.isEmpty()) {
            int size = queue.size();
            for (int i = 0; i < size; i++) {
                Node<K, V> current = queue.poll();
                
                // Validate key order
                for (int j = 0; j < current.pairs.size() - 1; j++) {
                    K key1 = current.pairs.get(j).key;
                    K key2 = current.pairs.get(j + 1).key;
                    assertTrue(key1.compareTo(key2) <= 0,
                            "Keys out of order at level " + level + ": " + key1 + " > " + key2);
                }

                // Validate key count
                if (current != tree.getRoot()) {
                    assertTrue(current.pairs.size() >= (tree.getOrder() / 2) - 1,
                            "Underflow at level " + level + " should be "+((tree.getOrder() / 2) - 1)+" but is : " + current.pairs.size());
                }
                assertTrue(current.pairs.size() <= tree.getOrder() - 1,
                        "Overflow at level " + level + ": " + current.pairs.size());

                // Validate children
                if (!current.isLeaf) {
                    assertEquals(current.children.size(), current.pairs.size() + 1,
                            "Children count mismatch");
                    
                    for (Node<K, V> child : current.children) {
                        assertSame(child.parent, current, "Parent pointer mismatch");
                        queue.add(child);
                    }
                }
            }
            level++;
        }
        
        validateLeafChain(tree);
    }

    private <K extends Comparable<K>, V> void validateLeafChain(BPlusTree<K, V> tree) {
        Node<K, V> leaf = tree.getFirst();
        Node<K, V> current = leaf;
        K lastKey = null;

        // Collect all leaves via BFS
        Set<Node<K, V>> allLeaves = new HashSet<>();
        Queue<Node<K, V>> queue = new LinkedList<>();
        queue.add(tree.getRoot());

        while (!queue.isEmpty()) {
            Node<K, V> node = queue.poll();
            if (node.isLeaf) {
                allLeaves.add(node);
            } else {
                queue.addAll(node.children);
            }
        }
        // Traverse leaf chain
        while (current != null) {
            // Verify this leaf is in the BFS set
            assertTrue(allLeaves.contains(current), "Leaf not found in BFS traversal");
            allLeaves.remove(current);
            for (Pair<K, V> pair : current.pairs) {
                if (lastKey != null) {
                    assertTrue(lastKey.compareTo(pair.key) <= 0,
                            "Leaf keys out of order: " + lastKey + " > " + pair.key);
                }
                lastKey = pair.key;
            }
            current = current.next;
        }

        // Ensure all leaves are connected
        assertTrue(allLeaves.isEmpty(), "Leaf chain missing " + allLeaves.size() + " leaves");
    }

    // ================ UNIQUE KEY TESTS ================
    @Test
    void testUniqueIntegerKeys() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        
        // Insert sequential keys
        for (int i = 100; i <= 1000; i += 100) {
            tree.insert(i, "Value" + i);
        }
        validateTreeStructure(tree);

        // Verify inserts
        for (int i = 100; i <= 1000; i += 100) {
            assertEquals("Value" + i, tree.search(i).value, "Key not found: " + i);
        }

        // Test updates
        tree.update(500, "Updated500");
        assertEquals("Updated500", tree.search(500).value, "Update failed");
        
        // Test deletions
        tree.remove(100, "Value100");
        tree.remove(1000, "Value1000");
        tree.remove(500, "Updated500");
        assertNull(tree.search(100), "Delete failed");
        assertNull(tree.search(1000), "Delete failed");
        assertNull(tree.search(500), "Delete failed");
        validateTreeStructure(tree);

        // Test range search
        List<Pair<Integer, String>> range = tree.rangeSearch(200, 800);
        assertEquals(6, range.size(), "Range search count mismatch");
        for (Pair<Integer, String> p : range) {
            assertTrue(p.key >= 200 && p.key <= 800, "Key out of range: " + p.key);
        }
    }

    @Test
    void testUniqueStringKeys() {
        BPlusTree<String, Integer> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        
        String[] keys = {"apple", "banana", "cherry", "date", "elderberry", "fig"};
        
        // Insert
        for (int i = 0; i < keys.length; i++) {
            tree.insert(keys[i], i);
        }
        validateTreeStructure(tree);

        // Verify
        for (int i = 0; i < keys.length; i++) {
            assertEquals(i, tree.search(keys[i]).value, "Key not found: " + keys[i]);
        }

        // Test deletions
        tree.remove("banana", 1);
        tree.remove("date", 3);
        assertNull(tree.search("banana"), "Delete failed");
        assertNull(tree.search("date"), "Delete failed");
        validateTreeStructure(tree);

        // Test range search
        List<Pair<String, Integer>> range = tree.rangeSearch("a", "d");
        assertEquals(2, range.size(), "Range search count mismatch");
        assertEquals("apple", range.get(0).key);
        assertEquals("cherry", range.get(1).key);
    }

    // ================ NON-UNIQUE KEY TESTS ================
    @Test
    void testNonUniqueIntegerKeys() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(false);
        
        // Insert duplicates
        tree.insert(100, "Value100-1");
        tree.insert(100, "Value100-2");
        tree.insert(200, "Value200");
        tree.insert(100, "Value100-3");
        validateTreeStructure(tree);

        // Verify primary values
        assertEquals("Value100-1", tree.search(100).get("Value100-1"), "Primary value mismatch");
        
        // Test value removal
        tree.remove(100, "Value100-1");
        assertEquals("Value100-2", tree.search(100).get("Value100-2"), "Primary value not updated");
        
        tree.remove(100, "Value100-2");
        assertEquals("Value100-3", tree.search(100).get("Value100-3"), "Primary value not updated");
        
        tree.remove(100, "Value100-3");
        assertNull(tree.search(100), "Key should be removed");
    }

    @Test
    void testNonUniqueStringKeys() {
        BPlusTree<String, Integer> tree = new BPlusTree<>(ORDER);
        tree.setUnique(false);
        
        // Insert duplicates
        tree.insert("A", 1);
        tree.insert("A", 2);
        tree.insert("B", 3);
        tree.insert("A", 4);
        tree.insert("B", 5);
        validateTreeStructure(tree);

        // Verify
        assertEquals(Integer.valueOf(1), tree.search("A").get(1), "Primary value mismatch");
        assertEquals(Integer.valueOf(3), tree.search("B").get(3), "Primary value mismatch");
        
        // Test deletions
        tree.remove("A", 1);
        assertEquals(Integer.valueOf(2), tree.search("A").get(2), "Primary value not updated");
        
        tree.remove("B", 3);
        assertEquals(Integer.valueOf(5), tree.search("B").get(5), "Primary value not updated");
        
        tree.remove("B", 5);
        assertNull(tree.search("B"), "Key should be removed");
    }

    // ================ EDGE CASE TESTS ================
    @Test
    void testEmptyTreeOperations() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        
        assertNull(tree.search(100), "Search on empty tree");
        assertTrue(tree.rangeSearch(100, 200).isEmpty(), "Range search on empty tree");
        tree.remove(100, "Value");  // Should not throw
        tree.update(100, "Value",null); // Should not throw
    }

    @Test
    void testRootOperations() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        
        // Insert until root splits
        tree.insert(10, "A");
        tree.insert(20, "B");
        tree.insert(30, "C");
        tree.insert(40, "D");  // Should cause root split
        validateTreeStructure(tree);
        assertFalse(tree.getRoot().isLeaf, "Root should be internal after split");

        // Delete until tree shrinks
        tree.remove(10, "A");
        tree.remove(20, "B");
        tree.remove(30, "C");
        tree.remove(40, "D");
        assertTrue(tree.getRoot().isLeaf, "Root should be leaf after deletions");
        assertTrue(tree.getRoot().pairs.isEmpty(), "Root should be empty");
    }

    @Test
    void testMinAndMaxKeys() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(4);  // Min keys = 1
        tree.setUnique(true);
        
        // Insert
        tree.insert(1, "A");
        tree.insert(2, "B");
        tree.insert(3, "C");  // Force split
        validateTreeStructure(tree);
        
        // Delete to cause underflow
        tree.remove(1, "A");
        tree.remove(2, "B");
        validateTreeStructure(tree);  // Should handle underflow
    }

    @Test
    void testDuplicatePrimaryValueUpdate() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(false);
        
        tree.insert(100, "Primary");
        tree.insert(100, "Duplicate1");
        tree.insert(100, "Duplicate2");
        
        // Update primary value
        tree.update(100, "UpdatedPrimary","Primary");
        assertEquals("UpdatedPrimary", tree.search(100).value, "Primary update failed");
        
        // Verify duplicates preserved
        tree.remove(100, "Duplicate1");
        tree.remove(100, "Duplicate2");
        assertEquals("UpdatedPrimary", tree.search(100).value, "Primary should remain");
    }

    // ================ RANDOMIZED STRESS TEST ================
    @Test
    void testRandomOperationsIntegerKeys() {
        randomOperationsTest(new BPlusTree<Integer, String>(ORDER), 1000);
    }

    @Test
    void testRandomOperationsStringKeys() {
        BPlusTree<String, Integer> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        List<String> insertedKeys = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            String key;
            do {
                key = generateRandomString(5);
            } while (!uniqueKeys.add(key)); // Ensure unique keys

            tree.insert(key, i);
            insertedKeys.add(key);
            validateTreeStructure(tree);
        }

        // Insert 1000 random keys
        for (int i = 0; i < 1000; i++) {
            String key = generateRandomString(5);
            tree.insert(key, i);
            insertedKeys.add(key);
            validateTreeStructure(tree);
        }
        
        // Verify all keys exist
        Collections.sort(insertedKeys);
        for (String key : insertedKeys) {
            assertNotNull(tree.search(key), "Key missing: " + key);
        }
        /* 
        // Random updates
        for (int i = 0; i < 200; i++) {
            int idx = random.nextInt(insertedKeys.size());
            String key = insertedKeys.get(idx);
            int newValue = random.nextInt(10000);
            tree.update(key, newValue);
            assertEquals(newValue, (int) tree.search(key), "Update failed: " + key);
        }*/
        
        // Random deletions
        for (int i = 0; i < 500; i++) {
            int idx = random.nextInt(insertedKeys.size());
            String key = insertedKeys.remove(idx);
            tree.remove(key, tree.search(key).value);
            assertNull(tree.search(key), "Delete failed: " + key);
            validateTreeStructure(tree);
        }
    }
    
    private void randomOperationsTest(BPlusTree<Integer, String> tree, int size) {
        tree.setUnique(true);
        List<Integer> insertedKeys = new ArrayList<>();
        
        // Insert random keys
        for (int i = 0; i < size; i++) {
            int key = random.nextInt(10000);
            if(tree.search(key)==null){
                tree.insert(key, "Value" + key);
                insertedKeys.add(key);
            }else{
                i--;
            }
            if (i % 100 == 0) validateTreeStructure(tree);
        }
        
        // Verify all keys exist
        Collections.sort(insertedKeys);
        for (int key : insertedKeys) {
            assertNotNull(tree.search(key), "Key missing: " + key);
        }
        /* 
        // Random updates
        for (int i = 0; i < size / 5; i++) {
            int idx = random.nextInt(insertedKeys.size());
            int key = insertedKeys.get(idx);
            tree.update(key, "Updated" + key);
            assertEquals("Updated" + key, tree.search(key), "Update failed: " + key);
        }*/
        
        // Random deletions
        for (int i = 0; i < size / 2; i++) {
            int idx = random.nextInt(insertedKeys.size());
            int key = insertedKeys.remove(idx);
            tree.remove(key, tree.search(key).value);
            assertNull(tree.search(key), "Delete failed: " + key);
            if (i % 50 == 0) validateTreeStructure(tree);
        }
        
        // Final validation
        validateTreeStructure(tree);
    }
    
    private String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    // ================ RANGE SEARCH TESTS ================
    @Test
    void testFullRangeSearchesInteger() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        
        for (int i = 0; i < 100; i++) {
            tree.insert(i, "Val" + i);
        }
        
        // Full range
        List<Pair<Integer, String>> all = tree.rangeSearch(null, null);
        assertEquals(100, all.size());
        for (int i = 0; i < 100; i++) {
            assertEquals(i, all.get(i).key.intValue());
        }
        
        // Lower bound only
        List<Pair<Integer, String>> upperHalf = tree.rangeSearch(50, null);
        assertEquals(50, upperHalf.size());
        assertEquals(50, upperHalf.get(0).key.intValue());
        assertEquals(99, upperHalf.get(49).key.intValue());
        
        // Upper bound only
        List<Pair<Integer, String>> lowerHalf = tree.rangeSearch(null, 49);
        assertEquals(50, lowerHalf.size());
        assertEquals(0, lowerHalf.get(0).key.intValue());
        assertEquals(49, lowerHalf.get(49).key.intValue());
    }

    @Test
    void testFullRangeSearchesString() {
        BPlusTree<String, Integer> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        
        String[] keys = {"apple", "banana", "cherry", "date", "elderberry", "fig"};
        for (int i = 0; i < keys.length; i++) {
            tree.insert(keys[i], i);
        }
        
        // Full range
        List<Pair<String, Integer>> all = tree.rangeSearch(null, null);
        assertEquals(6, all.size());
        for (int i = 0; i < 6; i++) {
            assertEquals(keys[i], all.get(i).key);
        }
        
        // Lower bound only
        List<Pair<String, Integer>> upperHalf = tree.rangeSearch("c", null);
        assertEquals(4, upperHalf.size());
        assertEquals("cherry", upperHalf.get(0).key);
        
        // Upper bound only
        List<Pair<String, Integer>> lowerHalf = tree.rangeSearch(null, "date");
        assertEquals(4, lowerHalf.size());
        assertEquals("date", lowerHalf.get(3).key);
    }

    @Test
    void testRangeSearchEdgeCases() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        
        tree.insert(10, "A");
        tree.insert(20, "B");
        tree.insert(30, "C");
        tree.insert(40, "D");
        
        // Range with no results
        List<Pair<Integer, String>> empty = tree.rangeSearch(50, 60);
        assertTrue(empty.isEmpty());
        
        // Single element range
        List<Pair<Integer, String>> single = tree.rangeSearch(20, 20);
        assertEquals(1, single.size());
        assertEquals(20, single.get(0).key.intValue());
        
        // Range covering all elements
        List<Pair<Integer, String>> all = tree.rangeSearch(10, 40);
        assertEquals(4, all.size());
    }

    @Test
    void testRangeSearchNonExistentBounds() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        
        tree.insert(15, "A");
        tree.insert(25, "B");
        tree.insert(35, "C");
        
        // Lower bound between keys
        List<Pair<Integer, String>> range1 = tree.rangeSearch(20, 30);
        assertEquals(1, range1.size());
        assertEquals(25, range1.get(0).key.intValue());
        
        // Upper bound between keys
        List<Pair<Integer, String>> range2 = tree.rangeSearch(10, 20);
        assertEquals(1, range2.size());
        assertEquals(15, range2.get(0).key.intValue());
        
        // Both bounds between keys
        List<Pair<Integer, String>> range3 = tree.rangeSearch(16, 26);
        assertEquals(1, range3.size());
        assertEquals(25, range3.get(0).key.intValue());
    }

    @Test
    void testNonUniqueMultipleDuplicates() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(false);

        // Insert multiple duplicates
        tree.insert(100, "Primary");
        tree.insert(100, "Dup1");
        tree.insert(100, "Dup2");
        tree.insert(100, "Dup3");

        // Remove duplicates one by one
        tree.remove(100, "Dup1");
        tree.remove(100, "Dup2");
        tree.remove(100, "Dup3");

        // Verify primary remains
        assertEquals("Primary", tree.search(100).value);
        validateTreeStructure(tree);
    }

    @Test
    void testMixedOperations() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        Set<Integer> inserted = new HashSet<>();
        Random rand = new Random();

        // Perform 500 random operations
        for (int i = 0; i < 500; i++) {
            int op = rand.nextInt(3);
            int key = rand.nextInt(100);

            switch (op) {
                case 0: // Insert
                    if (!inserted.contains(key)) {
                        tree.insert(key, "Val" + key);
                        inserted.add(key);
                    }
                    break;
                case 1: // Delete
                    if (inserted.remove(key)) {
                        tree.remove(key, "Val" + key);
                    }
                    break;
                case 2: // Update
                    if (inserted.contains(key)) {
                        tree.update(key, "Updated" + key);
                    }
                    break;
            }
            if (i % 50 == 0)
                validateTreeStructure(tree);
        }
        validateTreeStructure(tree);
    }

    @Test
    void testDeleteNonExistentKey() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);

        tree.insert(100, "Value100");
        tree.remove(200, "Value200"); // Non-existent key
        tree.remove(100, "WrongValue"); // Wrong value

        // Original key should remain
        assertNotNull(tree.search(100));
        validateTreeStructure(tree);
    }

    @Test
    void testUpdateNonExistentKey() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);

        tree.update(100, "NewValue"); // Non-existent key
        assertNull(tree.search(100));
        validateTreeStructure(tree);
    }

    @Test
    void testSizeTrackingUnique() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);

        // Test initial size
        assertEquals(0, tree.size());

        // Test insertions
        tree.insert(100, "A");
        tree.insert(200, "B");
        assertEquals(2, tree.size());

        // Test duplicate insert (should be ignored)
        tree.insert(100, "A");
        assertEquals(2, tree.size());

        // Test deletions
        tree.remove(100, "A");
        assertEquals(1, tree.size());

        // Test non-existent key removal
        tree.remove(300, "C");
        assertEquals(1, tree.size());
    }

    @Test
    void testSizeTrackingNonUnique() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(false);

        // Test initial size
        assertEquals(0, tree.size());

        // Test insertions
        tree.insert(100, "A");
        tree.insert(100, "B");
        tree.insert(200, "C");
        assertEquals(3, tree.size());

        // Test duplicate insertion with same Key and Value (should not add new value)
        tree.insert(100, "A");
        assertEquals(4, tree.size());

        // Test deletions
        tree.remove(100, "A");
        assertEquals(3, tree.size());

        // Test primary value promotion
        tree.remove(100, "B");
        assertEquals(2, tree.size());
    }

    @Test
    void testUpdateUniqueKeys() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);
        tree.insert(100, "A");
        tree.insert(200, "B");

        // Valid update
        tree.update(100, "Updated");
        assertEquals("Updated", tree.search(100).value);

        // Update non-existent key
        tree.update(300, "New");
        assertNull(tree.search(300));

        // Verify size unchanged
        assertEquals(2, tree.size());
    }

    @Test
    void testUpdateNonUniqueKeys() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(false);
        tree.insert(100, "Primary");
        tree.insert(100, "Duplicate");

        // Valid update
        tree.update(100, "NewPrimary", "Primary");
        assertEquals("NewPrimary", tree.search(100).value);

        // Update duplicate
        tree.update(100, "NewDuplicate", "Duplicate");
        assertTrue(tree.search(100).getDuplicates().contains(new Pair<>(100,"NewDuplicate")));

        // Invalid update (old value mismatch)
        tree.update(100, "Invalid", "WrongValue");
        assertFalse(tree.search(100).value.equals("Invalid"));

        // Verify size unchanged
        assertEquals(2, tree.size());
    }

    @Test
    void testUpdateExceptions() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);

        // Unique tree with oldValue specified
        tree.setUnique(true);
        tree.insert(100, "A");
        assertThrows(IllegalStateException.class, () -> tree.update(100, "B", "A"));

        // Non-unique tree without oldValue
        tree.setUnique(false);
        assertThrows(IllegalStateException.class, () -> tree.update(100, "C"));
    }

    @Test
    void testRangeSearchOpenEnded() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);

        for (int i = 1; i <= 100; i++) {
            tree.insert(i, "Val" + i);
        }

        // Lower bound only
        List<Pair<Integer, String>> lowerRange = tree.rangeSearch(90, null);
        assertEquals(11, lowerRange.size());
        assertEquals(90, lowerRange.get(0).key.intValue());
        assertEquals(100, lowerRange.get(10).key.intValue());

        // Upper bound only
        List<Pair<Integer, String>> upperRange = tree.rangeSearch(null, 10);
        assertEquals(10, upperRange.size());
        assertEquals(1, upperRange.get(0).key.intValue());
        assertEquals(10, upperRange.get(9).key.intValue());

        // Empty range
        List<Pair<Integer, String>> emptyRange = tree.rangeSearch(200, 300);
        assertTrue(emptyRange.isEmpty());
    }

    @Test
    void testRangeSearchPartialOverlap() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);
        tree.setUnique(true);

        tree.insert(10, "A");
        tree.insert(20, "B");
        tree.insert(30, "C");
        tree.insert(40, "D");

        // Partial overlap
        List<Pair<Integer, String>> range = tree.rangeSearch(15, 35);
        assertEquals(2, range.size());
        assertEquals(20, range.get(0).key.intValue());
        assertEquals(30, range.get(1).key.intValue());
    }

    @Test
    void testToString() {
        BPlusTree<Integer, String> tree = new BPlusTree<>(ORDER);

        // Empty tree
        assertTrue(tree.toString().contains("Tree is empty"));

        // Non-empty tree
        tree.insert(10, "A");
        tree.insert(20, "B");
        String output = tree.toString();

        // Verify basic structure
        assertTrue(output.contains("Level"));
        assertTrue(output.contains("Leaf Chain"));
        assertTrue(output.contains("10"));
        assertTrue(output.contains("20"));

        // Verify no exceptions
        assertDoesNotThrow(() -> tree.toString());
    }

    @Test
    void testMinKeyConfiguration() {
        // Test different orders
        for (int order = 3; order <= 7; order++) {
            BPlusTree<Integer, String> tree = new BPlusTree<>(order);
            int expectedMin = (int) Math.ceil((double) order / 2) - 1;

            // Insert until root splits
            for (int i = 1; i <= order; i++) {
                tree.insert(i, "Val" + i);
            }

            // Delete until underflow
            for (int i = 1; i <= expectedMin; i++) {
                tree.remove(i, "Val" + i);
                validateTreeStructure(tree);
            }
        }
    }
}
package com.database.db.bPlusTree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class BPlusTreeTest {

    private BPlusTree<Integer,Integer> tree;
    private Comparator<Pair<?,?>> comparator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        // Initialize the tree with an order of 5 for testing.
        comparator = (pair1, pair2) -> {
            Object key1 = pair1.getKey();
            Object key2 = pair2.getKey();
            // Compare based on key types
            if (key1 instanceof Comparable) {
                return ((Comparable<Object>) key1).compareTo(key2);
            }
            throw new IllegalArgumentException("Keys must implement Comparable.");
        };
        tree = new Tree<>(5);
    }

    @Test
    public void testInsertionAndSearch() {
        // Insert elements
        tree.insert(10, 1);
        tree.insert(20, 2);
        tree.insert(5, 3);
        tree.insert(15, 4);
        tree.insert(25, 5);
        tree.insert(30, 6);
        tree.insert(11, 7);
        tree.insert(22, 8);
        tree.insert(6, 9);
        tree.insert(16, 10);
        tree.insert(26, 11);
        tree.insert(31, 12);

        // Assert that the keys are inserted correctly by searching for each key
        assertTrue(tree.isKey(10));
        assertTrue(tree.isKey(20));
        assertTrue(tree.isKey(5));
        assertTrue(tree.isKey(15));
        assertTrue(tree.isKey(25));
        assertTrue(tree.isKey(30));
        assertTrue(tree.isKey(11));
        assertTrue(tree.isKey(22));
        assertTrue(tree.isKey(6));
        assertTrue(tree.isKey(16));
        assertTrue(tree.isKey(26));
        assertTrue(tree.isKey(31));
    }

    @Test
    public void testRangeQuery() {
        // Insert elements
        tree.insert(10, 1);
        tree.insert(20, 2);
        tree.insert(5, 3);
        tree.insert(15, 4);
        tree.insert(25, 5);
        tree.insert(30, 6);
        tree.insert(11, 7);
        tree.insert(22, 8);
        tree.insert(6, 9);
        tree.insert(16, 10);
        tree.insert(26, 11);
        tree.insert(31, 12);

        // Perform a range query and check if the range is correct
        int lower = 10;
        int upper = 25;
        List<Pair<Integer,Integer>> result = tree.rangeSearch(lower, upper);
        Pair<?,Integer> pairLow = new Pair<>(lower, 0);
        Pair<?,Integer> pairUp = new Pair<>(upper, 0);

        // The expected output should contain keys in the range [10, 25]
        assertNotNull(result);
        assertEquals(7, result.size());  // There should be 9 keys in the range [10, 25]

        // Check if all keys in result are within the range [10, 25]
        for (Pair<Integer,Integer> key : result) {
            assertTrue(comparator.compare(key, pairLow) >= 0 && comparator.compare(key, pairUp) <= 0);
        }
    }

    @Test
    public void testRemove() {
        // Insert elements
        tree.insert(10, 1);
        tree.insert(20, 2);
        tree.insert(5, 3);
        tree.insert(15, 4);
        tree.insert(25, 5);
        tree.insert(30, 6);

        // Remove an element
        int keyToRemove = 15;
        tree.remove(keyToRemove);

        // Assert that the key is no longer in the tree
        assertFalse(tree.isKey(keyToRemove));

        // Also check that other elements are still present
        assertTrue(tree.isKey(10));
        assertTrue(tree.isKey(20));
        assertTrue(tree.isKey(5));
        assertTrue(tree.isKey(25));
        assertTrue(tree.isKey(30));
    }

    @Test
    void testRandomInsert400times(){
        Random random = new Random();
        int i = 0;
        while (i < 400) {
            int num = random.nextInt();
            if (!tree.isKey(num)) {
                tree.insert(num, i);
                i++;
            }
        }
        //TODO check if leaf node keys are sorted.
    }

    @Test
    void testRemoveWithMerge() {
        int key1 = 1;
        int key2 = 2;
        int key3 = 3;
        int key4 = 4;
        
        tree.insert(key1, 1);
        tree.insert(key2, 2);
        tree.insert(key3, 3);
        tree.insert(key4, 4);
        
        // Remove key1, which should trigger a merge
        tree.remove(key1);
        
        // Verify the structure after merge
        assertFalse(tree.isKey(key1));
        assertTrue(tree.isKey(key2));
        assertTrue(tree.isKey(key3));
        assertTrue(tree.isKey(key4));
    }

    @Test
    public void testEmptyTree() {
        // Test an empty tree (no insertions yet)
        assertFalse(tree.isKey(10));
    }

    @Test
    public void testInsertAndPrintTree() {
        // Insert elements
        tree.insert(10, 1);
        tree.insert(20, 2);
        tree.insert(30, 3);

        // Optionally, print tree structure
        tree.printTree();  // This will print the tree structure for debugging purposes

        // You may also assert the tree structure based on expected behavior
        assertTrue(tree.isKey(10));
        assertTrue(tree.isKey(20));
        assertTrue(tree.isKey(30));
    }

    @Test
    void testSearchNotFound() {
        int key1 = 1;
        int key2 = 2;
        
        tree.insert(key1, 1);
        tree.insert(key2, 2);
        
        // Searching for a key that doesn't exist
        int keyNotFound = 3;
        assertFalse(tree.isKey(keyNotFound));
    }

    @Test
    void testOrderValidation() {
        // Test that an exception is thrown if the order is less than 3
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new Tree<>(2);
        });
        assertEquals("BPlus Tree Order must be at least 3.", exception.getMessage());
    }
}


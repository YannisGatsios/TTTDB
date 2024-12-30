package com.database.db.bPlusTree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.db.bPlusTree.TreeUtils.Pair;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class BPlusTreeTest {

    private BPlusTree tree;

    @BeforeEach
    public void setUp() {
        // Initialize the tree with an order of 5 for testing.
        tree = new BPlusTree(5);
    }

    @Test
    public void testInsertionAndSearch() {
        // Insert elements
        tree.insert(new Pair<byte[],Integer>(new byte[] {10}, 1));
        tree.insert(new Pair<byte[],Integer>(new byte[] {20}, 2));
        tree.insert(new Pair<byte[],Integer>(new byte[] {5}, 3));
        tree.insert(new Pair<byte[],Integer>(new byte[] {15}, 4));
        tree.insert(new Pair<byte[],Integer>(new byte[] {25}, 5));
        tree.insert(new Pair<byte[],Integer>(new byte[] {30}, 6));
        tree.insert(new Pair<byte[],Integer>(new byte[] {11}, 7));
        tree.insert(new Pair<byte[],Integer>(new byte[] {22}, 8));
        tree.insert(new Pair<byte[],Integer>(new byte[] {6}, 9));
        tree.insert(new Pair<byte[],Integer>(new byte[] {16}, 10));
        tree.insert(new Pair<byte[],Integer>(new byte[] {26}, 11));
        tree.insert(new Pair<byte[],Integer>(new byte[] {31}, 12));

        // Assert that the keys are inserted correctly by searching for each key
        assertTrue(tree.search(new byte[] {10}));
        assertTrue(tree.search(new byte[] {20}));
        assertTrue(tree.search(new byte[] {5}));
        assertTrue(tree.search(new byte[] {15}));
        assertTrue(tree.search(new byte[] {25}));
        assertTrue(tree.search(new byte[] {30}));
        assertTrue(tree.search(new byte[] {11}));
        assertTrue(tree.search(new byte[] {22}));
        assertTrue(tree.search(new byte[] {6}));
        assertTrue(tree.search(new byte[] {16}));
        assertTrue(tree.search(new byte[] {26}));
        assertTrue(tree.search(new byte[] {31}));
    }

    @Test
    public void testRangeQuery() {
        // Insert elements
        tree.insert(new Pair<byte[],Integer>(new byte[] {10}, 1));
        tree.insert(new Pair<byte[],Integer>(new byte[] {20}, 2));
        tree.insert(new Pair<byte[],Integer>(new byte[] {5}, 3));
        tree.insert(new Pair<byte[],Integer>(new byte[] {15}, 4));
        tree.insert(new Pair<byte[],Integer>(new byte[] {25}, 5));
        tree.insert(new Pair<byte[],Integer>(new byte[] {30}, 6));
        tree.insert(new Pair<byte[],Integer>(new byte[] {11}, 7));
        tree.insert(new Pair<byte[],Integer>(new byte[] {22}, 8));
        tree.insert(new Pair<byte[],Integer>(new byte[] {6}, 9));
        tree.insert(new Pair<byte[],Integer>(new byte[] {16}, 10));
        tree.insert(new Pair<byte[],Integer>(new byte[] {26}, 11));
        tree.insert(new Pair<byte[],Integer>(new byte[] {31}, 12));

        // Perform a range query and check if the range is correct
        byte[] lower = new byte[] {10};
        byte[] upper = new byte[] {25};
        List<Pair<byte[],Integer>> result = tree.rangeQuery(lower, upper);

        // The expected output should contain keys in the range [10, 25]
        assertNotNull(result);
        assertEquals(7, result.size());  // There should be 9 keys in the range [10, 25]

        // Check if all keys in result are within the range [10, 25]
        for (Pair<byte[],Integer> key : result) {
            assertTrue(Arrays.compare(key.getKey(), lower) >= 0 && Arrays.compare(key.getKey(), upper) <= 0);
        }
    }

    @Test
    public void testRemove() {
        // Insert elements
        tree.insert(new Pair<byte[],Integer>(new byte[] {10}, 1));
        tree.insert(new Pair<byte[],Integer>(new byte[] {20}, 2));
        tree.insert(new Pair<byte[],Integer>(new byte[] {5}, 3));
        tree.insert(new Pair<byte[],Integer>(new byte[] {15}, 4));
        tree.insert(new Pair<byte[],Integer>(new byte[] {25}, 5));
        tree.insert(new Pair<byte[],Integer>(new byte[] {30}, 6));

        // Remove an element
        byte[] keyToRemove = {15};
        tree.remove(keyToRemove);

        // Assert that the key is no longer in the tree
        assertFalse(tree.search(keyToRemove));

        // Also check that other elements are still present
        assertTrue(tree.search(new byte[] {10}));
        assertTrue(tree.search(new byte[] {20}));
        assertTrue(tree.search(new byte[] {5}));
        assertTrue(tree.search(new byte[] {25}));
        assertTrue(tree.search(new byte[] {30}));
    }

    @Test
    void testRandomIndert400times(){
        Random random = new Random();
        int i = 0;
        while (i < 400) {
            int num = random.nextInt(127);
            int num1 = random.nextInt(127);
            byte[] data = { (byte) num, (byte) num1 };
            if (!tree.search(data)) {
                tree.insert(new Pair<byte[],Integer>(data, i));
                i++;
            }
        }
    }

    @Test
    void testRemoveWithMerge() {
        Pair<byte[],Integer> key1 = new Pair<>(new byte[] {1}, 1);
        Pair<byte[],Integer> key2 = new Pair<>(new byte[] {2}, 2);
        Pair<byte[],Integer> key3 = new Pair<>(new byte[] {3}, 3);
        Pair<byte[],Integer> key4 = new Pair<>(new byte[] {4}, 4);
        
        tree.insert(key1);
        tree.insert(key2);
        tree.insert(key3);
        tree.insert(key4);
        
        // Remove key1, which should trigger a merge
        tree.remove(key1.getKey());
        
        // Verify the structure after merge
        assertFalse(tree.search(key1.getKey()));
        assertTrue(tree.search(key2.getKey()));
        assertTrue(tree.search(key3.getKey()));
        assertTrue(tree.search(key4.getKey()));
    }

    @Test
    public void testEmptyTree() {
        // Test an empty tree (no insertions yet)
        assertFalse(tree.search(new byte[] {10}));
    }

    @Test
    public void testInsertAndPrintTree() {
        // Insert elements
        tree.insert(new Pair<byte[],Integer>(new byte[] {10}, 1));
        tree.insert(new Pair<byte[],Integer>(new byte[] {20}, 2));
        tree.insert(new Pair<byte[],Integer>(new byte[] {30}, 3));

        // Optionally, print tree structure
        tree.printTree();  // This will print the tree structure for debugging purposes

        // You may also assert the tree structure based on expected behavior
        assertTrue(tree.search(new byte[] {10}));
        assertTrue(tree.search(new byte[] {20}));
        assertTrue(tree.search(new byte[] {30}));
    }

    @Test
    void testSearchNotFound() {
        Pair<byte[],Integer> key1 = new Pair<>(new byte[] {1}, 1);
        Pair<byte[],Integer> key2 = new Pair<>(new byte[] {2}, 2);
        
        tree.insert(key1);
        tree.insert(key2);
        
        // Searching for a key that doesn't exist
        byte[] keyNotFound = new byte[] {3};
        assertFalse(tree.search(keyNotFound));
    }

    @Test
    void testOrderValidation() {
        // Test that an exception is thrown if the order is less than 3
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new BPlusTree(2);
        });
        assertEquals("BPlus Tree Order must be at least 3.", exception.getMessage());
    }
}


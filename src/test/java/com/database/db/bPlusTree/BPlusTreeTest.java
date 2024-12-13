package com.database.db.bPlusTree;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;

public class BPlusTreeTest {

    @Test
    public void testInsert() {
        BPlusTree tree = new BPlusTree(3);

        // Insert keys
        tree.insert("key1".getBytes(StandardCharsets.UTF_8));
        tree.insert("key2".getBytes(StandardCharsets.UTF_8));
        tree.insert("key3".getBytes(StandardCharsets.UTF_8));
        tree.insert("key4".getBytes(StandardCharsets.UTF_8));

        // Verify tree structure using printTree (manual verification or use assertions)
        tree.printTree();

        // Check if all keys are present
        assertTrue(tree.search("key1".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key2".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key3".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key4".getBytes(StandardCharsets.UTF_8)));

        // Check if a non-existent key is not present
        assertFalse(tree.search("key5".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testPrintTree() {
        BPlusTree tree = new BPlusTree(3);

        // Insert keys
        tree.insert("key1".getBytes(StandardCharsets.UTF_8));
        tree.insert("key2".getBytes(StandardCharsets.UTF_8));
        tree.insert("key3".getBytes(StandardCharsets.UTF_8));
        tree.insert("key4".getBytes(StandardCharsets.UTF_8));

        // Print the tree
        System.out.println("Tree structure:");
        tree.printTree();

        // Add more asserts if necessary to verify node contents programmatically
    }

    @Test
    public void testRemove() {
        BPlusTree tree = new BPlusTree(3);

        // Insert keys
        tree.insert("key1".getBytes(StandardCharsets.UTF_8));
        tree.insert("key2".getBytes(StandardCharsets.UTF_8));
        tree.insert("key3".getBytes(StandardCharsets.UTF_8));
        tree.insert("key4".getBytes(StandardCharsets.UTF_8));

        // Remove a key and test
        tree.remove("key3".getBytes(StandardCharsets.UTF_8));
        assertFalse(tree.search("key3".getBytes(StandardCharsets.UTF_8))); // Key should no longer exist

        // Check remaining keys
        assertTrue(tree.search("key1".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key2".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key4".getBytes(StandardCharsets.UTF_8)));

        // Try removing a non-existent key (should not break)
        tree.remove("key5".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSearch() {
        BPlusTree tree = new BPlusTree(3);

        // Insert keys
        tree.insert("key1".getBytes(StandardCharsets.UTF_8));
        tree.insert("key2".getBytes(StandardCharsets.UTF_8));
        tree.insert("key3".getBytes(StandardCharsets.UTF_8));

        // Search for existing keys
        assertTrue(tree.search("key1".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key2".getBytes(StandardCharsets.UTF_8)));
        assertTrue(tree.search("key3".getBytes(StandardCharsets.UTF_8)));

        // Search for non-existent keys
        assertFalse(tree.search("key4".getBytes(StandardCharsets.UTF_8)));
        assertFalse(tree.search("nonexistent".getBytes(StandardCharsets.UTF_8)));
    }
}

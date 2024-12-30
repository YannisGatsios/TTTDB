package com.database.db.bPlusTree;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TreeUtils {
    public static class Pair<K, V> {
        private K key;
        private V value;
    
        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }
    
        public K getKey() {
            return key;
        }
    
        public V getValue() {
            return value;
        }
    
        public void setKey(K key) {
            this.key = key;
        }
    
        public void setValue(V value) {
            this.value = value;
        }
    }

    private Node getLeftMostLeaf(BPlusTree tree){
        Node current = tree.getRoot();
        while(!current.isLeaf){
            current = current.children.get(0);
        }
        return current;
    }

    public byte[] treeToBuffer(BPlusTree tree, int maxSizeOfKey, int numOfEtriesPerPage){
        int estimatedSize = Integer.BYTES; // For total number of pages
        Node node = this.getLeftMostLeaf(tree);
        while (node != null) {
            for (int i = 0; i < node.keys.size(); i++) {
                estimatedSize += Short.BYTES; // Key length
                estimatedSize += this.getKeySize(node.keys.get(i).getKey()); // Actual key size
                estimatedSize += Integer.BYTES; // Value size
            }
            node = node.next;
        }
        // Allocate buffer with the estimated size
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.putInt(tree.getLastPageID());
        //Adding the key value Pairs.
        //starting from the left most Leaf Node
        node = this.getLeftMostLeaf(tree);
        while (node.next != null) {
            for(int i = 0; i < node.keys.size(); i++){
                Object key = node.keys.get(i).getKey();
                int value = node.keys.get(i).getValue();

                buffer.putShort((short) this.getKeySize(key));
                buffer.put(this.keyToByteArray(key));
                buffer.putInt(value);
            }
            node = node.next;
        }
        buffer.putShort((short)0);//This will indicate that the file is over.
        buffer.flip();
        return buffer.array();
    }

    private int getKeySize(Object key){
        if(key instanceof String){
            return ((String) key).length();
        } else if(key instanceof Integer){
            return Integer.BYTES;
        } else if(key instanceof byte[]){
            return ((byte[]) key).length;
        }
        throw new IllegalArgumentException("Invalid key type(Can not read key size)");
    }

    public byte[] keyToByteArray(Object key) {
        switch (key.getClass().getSimpleName()) {
            case "Integer":
                ByteBuffer buffer = ByteBuffer.allocate(4); // Allocate 4 bytes
                buffer.putInt((int) key);
                return buffer.array();

            case "String":
                return ((String) key).getBytes(StandardCharsets.UTF_8);

            case "byte[]":
                return (byte[]) key;

            default:
                throw new IllegalArgumentException("Invalid Type Of ID (primary key).");
        }
    }

    public BPlusTree bufferToTree(byte[] treeBuffer, int treeOrder){
        BPlusTree newTree = new BPlusTree(treeOrder);
        newTree.setLastPageID(treeOrder = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 4)).getInt());
        treeBuffer = Arrays.copyOfRange(treeBuffer, 4, treeBuffer.length);

        short size = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 2)).getShort();
        treeBuffer = Arrays.copyOfRange(treeBuffer, 2, treeBuffer.length);
        while(size != 0){
            byte[] key = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, size)).array();
            treeBuffer = Arrays.copyOfRange(treeBuffer, size, treeBuffer.length);
            int value = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 4)).getInt();
            treeBuffer = Arrays.copyOfRange(treeBuffer, 4, treeBuffer.length);
            Pair<byte[], Integer> pair = new Pair<>(key, value);
            newTree.insert(pair);

            size = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 2)).getShort();
            treeBuffer = Arrays.copyOfRange(treeBuffer, 2, treeBuffer.length);
        }
        return newTree;
    }

    public void writeTree(String filePath, byte[] treeBuffer) {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            fos.write(treeBuffer);
            fos.getChannel().truncate(treeBuffer.length);
            System.out.println("Data successfully written to " + filePath);
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    public byte[] readTree(String indexPath) {
        byte[] data = null;
        try (FileInputStream fis = new FileInputStream(indexPath)) {
            // Get the file length
            long fileLength = fis.available();
            // Create a byte array to hold the file data
            data = new byte[(int) fileLength];
            // Read the file into the byte array
            fis.read(data);
            System.out.println("File successfully read into byte array.");
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
        return data;
    }
}

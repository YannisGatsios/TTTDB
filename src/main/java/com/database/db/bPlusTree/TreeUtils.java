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
                estimatedSize += this.getObjectSize(node.keys.get(i).getKey(),(byte)1); // Actual key size
                estimatedSize += this.getObjectSize(node.keys.get(i).getValue(), (byte)1); // Value size
            }
            node = node.next;
        }
        estimatedSize += Short.BYTES; //This accounts for the EOF flag that is a Short = 0
        // Allocate buffer with the estimated size
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        buffer.putInt(tree.getLastPageID());
        //Adding the key value Pairs.
        //starting from the left most Leaf Node
        node = this.getLeftMostLeaf(tree);
        while (node != null) {
            for(int i = 0; i < node.keys.size(); i++){
                Object key = node.keys.get(i).getKey();
                Object value = node.keys.get(i).getValue();

                buffer.putShort((short) this.getObjectSize(key, (byte)0));
                buffer.put(this.objectToByteArray(key));
                if(value instanceof Integer){
                    buffer.putInt((int)value);
                }else{
                    buffer.putShort((short)this.getObjectSize(value , (byte)0));
                    buffer.put(this.objectToByteArray(value));
                }
            }
            node = node.next;
        }
        buffer.putShort((short)0);//This will indicate that the file is over.
        buffer.flip();
        return buffer.array();
    }

    //Account size must only be 1 or 0
    private int getObjectSize(Object key, byte accountSize){
        if(key instanceof String){
            return ((String) key).length() + (accountSize*Short.BYTES);
        } else if(key instanceof Integer){
            return Integer.BYTES;
        } else if(key instanceof byte[]){
            return ((byte[]) key).length + (accountSize*Short.BYTES);
        }
        throw new IllegalArgumentException("Invalid key type(Can not read key size)");
    }

    private byte[] objectToByteArray(Object key) {
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

    public BPlusTree bufferToTree(byte[] treeBuffer, int treeOrder, String instaneOfKey){
        BPlusTree newTree = new BPlusTree(treeOrder);
        newTree.setLastPageID(treeOrder = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 4)).getInt());
        treeBuffer = Arrays.copyOfRange(treeBuffer, 4, treeBuffer.length);

        short size = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 2)).getShort();
        treeBuffer = Arrays.copyOfRange(treeBuffer, 2, treeBuffer.length);
        int ind = 0;
        while(size != 0){
            Object key;
            switch (instaneOfKey) {
                case "Integer":
                    key = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 4)).getInt();
                    treeBuffer = Arrays.copyOfRange(treeBuffer, 4, treeBuffer.length);
                    break;
                case "String":
                    key = new String(Arrays.copyOfRange(treeBuffer, 0, size), StandardCharsets.UTF_8);
                    treeBuffer = Arrays.copyOfRange(treeBuffer, size, treeBuffer.length);
                    break;
                case "Byte":
                    key = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, size)).array();
                    treeBuffer = Arrays.copyOfRange(treeBuffer, size, treeBuffer.length);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid primary key type.(From reading indexes)");
            }
            int value = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 4)).getInt();
            treeBuffer = Arrays.copyOfRange(treeBuffer, 4, treeBuffer.length);
            Pair<?, Integer> pair = new Pair<>(key, value);
            newTree.insert(pair);

            size = ByteBuffer.wrap(Arrays.copyOfRange(treeBuffer, 0, 2)).getShort();
            treeBuffer = Arrays.copyOfRange(treeBuffer, 2, treeBuffer.length);
            ind++;
        }
        System.out.println("Number Of Indexes read From Index File."+ind);
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

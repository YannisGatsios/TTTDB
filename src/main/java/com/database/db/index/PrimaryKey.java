package com.database.db.index;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.database.db.table.Table;

public class PrimaryKey<K extends Comparable<K>> extends BPlusTree<K,Integer> {

    public PrimaryKey(int order){
        super(order);
    }

    public byte[] treeToBuffer(int maxSizeOfPrimaryKey){
        if(this.getRoot() == null || this.getRoot().pairs.size() == 0) return new byte[0];
        int bufferSize = Integer.BYTES; // This takes into account the integer in the beginning of the file where the number of blocks that are saved is stored.
        Node<K,Integer> node = this.getLeftMostLeaf(this.getRoot());
        while (node != null) {
            for (int i = 0; i < node.pairs.size(); i++) {
                bufferSize += this.getObjectSize(node.pairs.get(i).key) + Short.BYTES; // Actual key size
                bufferSize += Integer.BYTES; // Value size is always an Integer and has the corresponding block value.
            }
            node = node.next;
        }
        bufferSize += Short.BYTES; //This accounts for the EOF flag that is a Short = 0
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.putInt(this.getNumberOfPages());//Adding the number of Pages of the table in the start of the file.
        //Adding the key value Pairs starting from the left most node.
        node = this.getLeftMostLeaf(this.getRoot());
        while (node != null) {
            for(int i = 0; i < node.pairs.size(); i++){
                K key = node.pairs.get(i).key;
                int value = node.pairs.get(i).value;
                byte[] keyBuffer = this.objectToByteArray(key);
                buffer.putShort((short) keyBuffer.length);
                buffer.put(keyBuffer);
                buffer.putInt(value);
            }
            node = node.next;
        }
        buffer.putShort((short)0);//This will indicate that the file is over.
        buffer.flip();
        return buffer.array();
    }

    //This method is used to get the left most leaf node of a B+Tree recursively.
    private Node<K,Integer> getLeftMostLeaf(Node<K,Integer> rootNode){
        if(rootNode.isLeaf){return rootNode;}
        return this.getLeftMostLeaf(rootNode.children.get(0));
    }
    //Account size must only be 1 or 0
    private int getObjectSize(Object key){
        switch (key.getClass().getSimpleName()) {
            case "Integer":
                return Integer.BYTES;
            case "String":
                return ((String) key).length();
            case "byte[]":
                return ((byte[]) key).length;
            default:
                throw new IllegalArgumentException("Invalid key type(Can not read key size).");
        }
    }private byte[] objectToByteArray(Object key) {
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
    //If the Index file is empty it returns a new empty B+Tree
    public PrimaryKey<K> bufferToTree(byte[] treeBuffer, Table table) {
        if (treeBuffer.length == 0) return new PrimaryKey<K>(table.getPageMaxNumOfEntries()); // Return empty tree if file is empty
        ByteBuffer buffer = ByteBuffer.wrap(treeBuffer); // Wrap byte array for efficient parsing
        PrimaryKey<K> newTree = new PrimaryKey<>(table.getPageMaxNumOfEntries());
        newTree.setNumberOfPages(buffer.getInt()); // Read the number of pages int the table.
        
        int ind = 0; // Counter for debug purposes
        short size = buffer.getShort(); // Read initial size
        while (size != 0) {
            K key = keyBufferToKey(buffer, table.getPrimaryKeyType(), size); // Deserialize key
            int value = buffer.getInt();
            newTree.insert(key, value); // Insert into the tree
            size = buffer.getShort(); // Read next size for the next iteration
            ind++;
        }
        System.out.println("Number of indexes read from Index file: " + ind + " entries.");
        return newTree;
    }
    @SuppressWarnings("unchecked")
    private K keyBufferToKey(ByteBuffer buffer, String typeOfKey, int size) {
        switch (typeOfKey) {
            case "Integer":
                return (K) (Integer) buffer.getInt(); // Deserialize as Integer
            case "String":
                byte[] strBytes = new byte[size];
                buffer.get(strBytes); // Read the specified number of bytes for String
                return (K) new String(strBytes, StandardCharsets.UTF_8);
            default:
                throw new IllegalArgumentException("Invalid primary key type. (From reading indexes)");
        }
    }
}

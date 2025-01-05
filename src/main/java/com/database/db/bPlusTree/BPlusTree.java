package com.database.db.bPlusTree;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.database.db.table.Table;

public abstract class BPlusTree<K extends Comparable<K>,V> implements BTree<K,V> {

    public abstract Node<K,V> getRoot();
    public abstract void setLastPageID(int lastPageID);
    public abstract int getLastPageID();
    public abstract void addOnePageID();
    public abstract void removeOnePageID();
    
    private Node<K,V> getLeftMostLeaf(BPlusTree<K,V> tree){
        Node<K,V> current = tree.getRoot();
        while(!current.isLeaf){
            current = current.children.get(0);
        }
        return current;
    }

    public byte[] treeToBuffer(BPlusTree<K,V> tree, int maxSizeOfKey){
        if(tree.getRoot() == null || tree.getRoot().keys.size() == 0) return new byte[0];
        int estimatedSize = Integer.BYTES; // For total number of pages
        Node<K,V> node = this.getLeftMostLeaf(tree);
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
                K key = node.keys.get(i).getKey();
                V value = node.keys.get(i).getValue();
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
    //If the Index file is empty it returns a new empty B+Tree
    @SuppressWarnings("unchecked")
    public BPlusTree<K, V> bufferToTree(byte[] treeBuffer, Table table) {
        if (treeBuffer.length == 0) return new Tree<>(table.getPageMaxNumOfEntries()); // Return empty tree if file is empty
        
        ByteBuffer buffer = ByteBuffer.wrap(treeBuffer); // Wrap byte array for efficient parsing
        BPlusTree<K, V> newTree = new Tree<>(table.getPageMaxNumOfEntries());
        newTree.setLastPageID(buffer.getInt()); // Read the last page ID
        
        int ind = 0; // Counter for debug purposes
        short size = buffer.getShort(); // Read initial size
        while (size != 0) {
            K key = deserializeKey(buffer, table.getIDtype(), size); // Deserialize key
            V value = (V) (Integer) buffer.getInt();
            newTree.insert(key, value); // Insert into the tree
            
            size = buffer.getShort(); // Read next size for the next iteration
            ind++;
        }
        
        System.out.println("Number of indexes read from Index file: " + ind + " entries.");
        return newTree;
    }

    @SuppressWarnings("unchecked")
    private K deserializeKey(ByteBuffer buffer, String instanceOfKey, int size) {
        switch (instanceOfKey) {
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

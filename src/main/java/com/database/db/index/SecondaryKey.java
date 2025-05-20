package com.database.db.index;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.database.db.table.Table;

public class SecondaryKey<K extends Comparable<K>,V> extends BPlusTree<K,V> {
    
    public SecondaryKey(int order){
        super(order);
    }

    public byte[] treeToBuffer(int maxSizeOfPrimaryKey){
        if(this.getRoot() == null || this.getRoot().pairs.size() == 0) return new byte[0];
        int bufferSize = 0;
        Node<K,V> node = this.getLeftMostLeaf(this);
        while (node != null) {
            for (int i = 0; i < node.pairs.size(); i++) {
                bufferSize += this.getObjectSize(node.pairs.get(i).key) + Short.BYTES; // Actual key size
                bufferSize += this.getObjectSize(node.pairs.get(i).value) + Short.BYTES; // Getting the Values size.
            }
            node = node.next;
        }
        bufferSize += Short.BYTES; //This accounts for the EOF flag that is a Short = 0
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        node = this.getLeftMostLeaf(this);
        while (node != null) {
            for(int i = 0; i < node.pairs.size(); i++){
                K key = node.pairs.get(i).key;
                V value = node.pairs.get(i).value;
                if (value instanceof Integer){
                    buffer.putInt((Integer)value);
                }else if(value instanceof String){
                    byte[] keyBuffer = this.objectToByteArray(key);
                    buffer.putShort((short) keyBuffer.length);
                    buffer.put(keyBuffer);
                }else{
                    throw new IllegalArgumentException("Invalid Value Type for Secondary Key (primary key).");
                }
                buffer.putInt((int)value);
            }
            node = node.next;
        }
        buffer.putShort((short)0);//This will indicate that the file is over.
        buffer.flip();
        return buffer.array();
    }
    private Node<K,V> getLeftMostLeaf(BPlusTree<K,V> tree){
        Node<K,V> current = tree.getRoot();
        while(!current.isLeaf){
            current = current.children.get(0);
        }
        return current;
    }private int getObjectSize(Object key){
        if(key instanceof String){
            return ((String) key).length();
        } else if(key instanceof byte[]){
            return ((byte[]) key).length;
        }
        throw new IllegalArgumentException("Invalid key type(Can not read key size)");
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
    public SecondaryKey<K,V> bufferToTree(byte[] treeBuffer, Table table){
        return null;
    }
}

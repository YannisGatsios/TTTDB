package com.database.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.database.db.table.Table;
import com.database.db.table.Type;
import com.database.db.table.Type.DeserializationResult;

public class SecondaryKey<K extends Comparable<K>,V> extends BPlusTree<K,V> {
    private int columnIndex;
    
    public SecondaryKey(Table<K> table, int columnIndex){
        super(table.getEntriesPerPage());
        this.setUnique(false);
        this.columnIndex = columnIndex;
    }

    public void initialize(Table<K> table){
    }

    public byte[] treeToBuffer(Table<K> table){
        if (this.getRoot() == null || this.getRoot().pairs.size() == 0) return new byte[0];
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                DataOutputStream dataStream = new DataOutputStream(byteStream)) {
            Node<K, V> node = this.getFirst();
            Type keyType = table.getSchema().getTypes()[this.columnIndex];
            Type valueType = table.getSchema().getTypes()[table.getPrimaryKeyColumnIndex()];
            while (node != null) {
                for (Pair<K, V> pair : node.pairs) {
                    // Serialize key
                    byte[] keyBytes = keyType.toBytes(pair.key);
                    dataStream.write(keyBytes);
                    // Serialize value
                    byte[] valueBytes = valueType.toBytes(pair.value);
                    if (!(pair.value instanceof Integer)) dataStream.writeShort(valueBytes.length);
                    dataStream.write(valueBytes);
                    // Serialize duplicates if any
                    if (pair.getDuplicates() != null) {
                        for (V duplicate : pair.getDuplicates()) {
                            dataStream.write(keyBytes);
                            valueBytes = valueType.toBytes(duplicate);
                            dataStream.write(valueBytes);
                        }
                    }
                }
                node = node.next;
            }
            // Write EOF marker
            dataStream.writeShort(0);
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Buffer conversion failed", e);
        }
    }
    public SecondaryKey<K,V> bufferToTree(byte[] bufferData, Table<K> table){
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("Buffer data cannot be null or empty.");
        int startIndex = 0;
        for (int i = 0; i < table.getPrimaryKey().size(); i++) {
            Type keyType = table.getSchema().getTypes()[this.columnIndex];
            DeserializationResult keyResult = keyType.fromBytes(bufferData, startIndex);
            K key = (K)keyResult.valueObject();
            startIndex = keyResult.nextIndex();

            Type valueType = table.getSchema().getTypes()[table.getPrimaryKeyColumnIndex()];
            DeserializationResult valueResult = valueType.fromBytes(bufferData, startIndex); 
            V value = (V) valueResult.valueObject();
            startIndex = valueResult.nextIndex();
            this.insert(key, value);
        }
        return null;
    }


    public int getColumnIndex(){return this.columnIndex;}
}

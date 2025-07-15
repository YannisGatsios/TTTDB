package com.database.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Table;
import com.database.db.table.Type;
import com.database.db.table.Type.DeserializationResult;

public class SecondaryKey<K extends Comparable<? super K>,V> extends BPlusTree<K,V> {
    private int columnIndex;
    
    public SecondaryKey(Table table, int columnIndex) throws InterruptedException, ExecutionException{
        super(table.getEntriesPerPage());
        this.setUnique(false);
        this.columnIndex = columnIndex;
        FileIO fileIO = new FileIO(table.getFileIOThread());
        byte[] treeBuffer = fileIO.readTree(table.getSKPath(this.columnIndex));
        if (treeBuffer == null || treeBuffer.length == 0) return;
        this.bufferToTree(treeBuffer, table);
    }

    public void initialize(Table table) throws InterruptedException, ExecutionException, IOException{
        int numberOfPages = table.getPages();
        if(numberOfPages == 0) return;
        TablePage page;
        ArrayList<Entry> list;
        for(int i = 0;i <= numberOfPages;i++){
            page = new TablePage(i, table);
            list = page.getAll();
            for (Entry entry : list) {
                this.insert((K)entry.get(this.columnIndex), (V)entry.get(this.columnIndex));
            }
        }
        byte[] treeBuffer = this.treeToBuffer(table);
        FileIO fileIO = new FileIO(table.getFileIOThread());
        fileIO.writeTree(table.getSKPath(this.columnIndex), treeBuffer);
    }

    public byte[] treeToBuffer(Table table){
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
                    if (valueType.getFixedSize() == -1) dataStream.writeShort(valueBytes.length);
                    dataStream.write(valueBytes);
                    // Serialize duplicates if any
                    if (pair.getDuplicates() != null) {
                        for (Pair<K,V> duplicate : pair.getDuplicates()) {
                            dataStream.write(keyBytes);
                            valueBytes = valueType.toBytes(duplicate.value);
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
    public void bufferToTree(byte[] bufferData, Table table){
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
    }


    public int getColumnIndex(){return this.columnIndex;}
}

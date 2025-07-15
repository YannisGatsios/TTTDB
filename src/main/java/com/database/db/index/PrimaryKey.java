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

public class PrimaryKey<K extends Comparable<? super K>> extends BPlusTree<K,Integer> {
    private int columnIndex;
    public PrimaryKey(Table table) throws InterruptedException, ExecutionException {
        super(table.getEntriesPerPage());
        this.columnIndex = table.getPrimaryKeyColumnIndex();
        FileIO fileIO = new FileIO(table.getFileIOThread());
        byte[] treeBuffer = fileIO.readTree(table.getPKPath());
        if (treeBuffer == null || treeBuffer.length == 0) return;
        this.bufferToTree(treeBuffer, table);
    }

    public void initialize(Table table) throws InterruptedException, ExecutionException, IOException {
        int numberOfPages = table.getPages();
        if(numberOfPages == 0) return;
        TablePage page;
        ArrayList<Entry> list;
        for(int i = 0;i <= numberOfPages;i++){
            page = new TablePage(i, table);
            list = page.getAll();
            for (Entry entry : list) {
                this.insert((K)entry.get(this.columnIndex), i);
            }
        }
        byte[] treeBuffer = this.treeToBuffer(table);
        FileIO fileIO = new FileIO(table.getFileIOThread());
        fileIO.writeTree(table.getPKPath(), treeBuffer);
    }

    public byte[] treeToBuffer(Table table){
        if (this.getRoot() == null || this.getRoot().pairs.size() == 0) return new byte[0];
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                DataOutputStream dataStream = new DataOutputStream(byteStream)) {
            Node<K, Integer> node = this.getFirst();
            Type keyType = table.getSchema().getTypes()[table.getSchema().getPrimaryKeyIndex()];
            Type valueType = table.getSchema().getTypes()[table.getPrimaryKeyColumnIndex()];
            while (node != null) {
                for (Pair<K, Integer> pair : node.pairs) {
                    // Serialize key
                    byte[] keyBytes = keyType.toBytes(pair.key);
                    dataStream.write(keyBytes);
                    // Serialize value
                    dataStream.writeInt(pair.value);
                    // Serialize duplicates if any
                    if (pair.getDuplicates() != null) {
                        for (Pair<K,Integer> duplicate : pair.getDuplicates()) {
                            dataStream.write(keyBytes);
                            dataStream.writeInt(duplicate.value);
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
        for (int i = 0; i < table.getPrimaryKey().size(); i++) {//TODO PK.size() makes no sense when we init PK
            Type keyType = table.getSchema().getTypes()[table.getSchema().getPrimaryKeyIndex()];
            DeserializationResult keyResult = keyType.fromBytes(bufferData, startIndex);
            K key = (K)keyResult.valueObject();
            startIndex = keyResult.nextIndex();

            Type valueType = table.getSchema().getTypes()[table.getPrimaryKeyColumnIndex()];
            DeserializationResult valueResult = valueType.fromBytes(bufferData, startIndex); 
            Integer value = (Integer)valueResult.valueObject();
            startIndex = valueResult.nextIndex();
            this.insert(key, value);
        }
    }
}

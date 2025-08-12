package com.database.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.InputMismatchException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.page.Entry;
import com.database.db.page.TablePage;
import com.database.db.table.Table;
import com.database.db.table.DataType;

public class BTreeSerialization<K extends Comparable<? super K>> extends BPlusTree<K,BTreeSerialization.BlockPointer>{
    protected int columnIndex;
    public record BlockPointer(int BlockID, short RowOffset){
        public static final int BYTES = 6;
        public byte[] toBytes(){
            ByteBuffer buffer = ByteBuffer.allocate(6);
            buffer.putInt(BlockID);
            buffer.putShort(RowOffset);
            return buffer.array();
        }
        public static BlockPointer fromBytes(ByteBuffer buffer) {
            int BlockID = buffer.getInt();
            short RowOffset = buffer.getShort();
            return new BlockPointer(BlockID, RowOffset);
        }
    }

    public BTreeSerialization(int order){
        super(order);
    }
    @SuppressWarnings("unchecked")
    public void initialize(Table table) throws InterruptedException, ExecutionException, IOException {
        int numberOfPages = table.getPages();
        if(numberOfPages == 0) return;
        TablePage page;
        Entry[] list;
        for(int i = 0;i < numberOfPages;i++){
            page = new TablePage(i, table);
            list = page.getAll();
            for (int j = 0; j < page.size(); j++) {
                Entry entry = list[j];
                if (entry == null) continue;
                K key = (K) entry.get(this.columnIndex);
                if (this.isUnique() && this.isKey(key))throw new IOException("Error initializing unique index, Table includes duplicate values");
                if (!this.isNullable() && key == null)throw new IOException("Error initializing not nullable index, Table includes null values");
                this.insert(key, new BlockPointer(i, (short) j));
            }
        }
        byte[] treeBuffer = this.toBytes(table);
        FileIO fileIO = new FileIO(table.getFileIOThread());
        fileIO.writeTree(table.getIndexPath(this.columnIndex), treeBuffer);
    }
    
    public byte[] toBytes(Table table){
        if (this.getRoot() == null || this.getRoot().pairs.size() == 0) return new byte[0];
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();DataOutputStream dataStream = new DataOutputStream(byteStream)) {
            Node<K, BlockPointer> node = this.getFirst();
            DataType keyType = table.getSchema().getTypes()[this.columnIndex];
            dataStream.writeLong(this.size());
            int numOfNulls = 0;
            if(this.isNullable()){
                numOfNulls = this.nullsToBytes(dataStream);
            }
            int index = 0;
            while (node != null) {
                for (Pair<K, BlockPointer> pair : node.pairs) {
                    this.pairToBytes(pair, dataStream, keyType);
                    index++;
                }
                node = node.next;
            }
            if(index+numOfNulls != this.size()) 
                throw new InputMismatchException("B+Tree size dose not much entries turned into bytes Expected: "+this.size()+" Actual: "+index+numOfNulls);
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Buffer conversion failed", e);
        }
    }

    private int nullsToBytes(DataOutputStream out) throws IOException {
        Pair<K, BlockPointer> pair = this.getNullPair();
        if(pair == null) {
            out.writeInt(0);
            return 0;
        }
        List<BlockPointer> nullEntries = pair.getValues();
        out.writeInt(nullEntries.size());
        for (BlockPointer value : nullEntries) {
            out.write(value.toBytes());
        }
        return nullEntries.size();
    }

    private void pairToBytes(Pair<K, BlockPointer> pair, DataOutputStream dataStream, DataType keyType) throws IOException {
        // Serialize key
        byte[] keyBytes = keyType.toBytes(pair.key);
        dataStream.write(keyBytes);
        // Serialize value
        dataStream.write(pair.value.toBytes());
        // Serialize duplicates if any
        if (!this.isUnique() && pair.getDuplicates() != null) {
            for (BlockPointer dup : pair.getDuplicates()) {
                dataStream.write(keyBytes);
                dataStream.write(dup.toBytes());
            }
        }
    }

    public void fromBytes(byte[] bufferData, Table table){
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("Buffer data cannot be null or empty.");
        ByteBuffer buffer = ByteBuffer.wrap(bufferData);
        long size = buffer.getLong();
        int numOfNulls = 0;
        if(this.isNullable()){
            numOfNulls = this.nullFromBytes(buffer);
        }
        Pair<K,BlockPointer> result;
        DataType keyType = table.getSchema().getTypes()[this.columnIndex];
        for (int i = 0; i < size-numOfNulls; i++) {
            result = this.pairFromBytes(buffer, keyType);
            this.insert(result.key, result.value);
        }
        if(this.size() != size) throw new IndexOutOfBoundsException("Corrupt Index file sizes don't much Expected:"+size+" Actual:"+this.size());
    }
    private int nullFromBytes(ByteBuffer buffer){
        int numOfNulls = buffer.getInt();
        for(int i = 0;i<numOfNulls;i++){
            K key = null;
            BlockPointer value = BlockPointer.fromBytes(buffer);
            this.insert(key, value);
        }
        return numOfNulls;
    }
    @SuppressWarnings("unchecked")
    private Pair<K, BlockPointer> pairFromBytes(ByteBuffer buffer, DataType keyType) {
        K key = (K) keyType.fromBytes(buffer);
        BlockPointer value = BlockPointer.fromBytes(buffer);
        return new Pair<>(key, value);
    }

    public void setColumnIndex(int columnIndex){this.columnIndex = columnIndex;}
    public int getColumnIndex(){return this.columnIndex;}
}
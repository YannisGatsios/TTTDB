package com.database.db.cache;

import java.util.ArrayList;
import java.util.List;

import com.database.db.index.BTreeSerialization;
import com.database.db.index.BTreeSerialization.PointerPair;

public class IndexSnapshot {
    public enum OperationEnum {
        INSERT,
        REMOVE,
        UPDATE
    }
    public record Operation(OperationEnum operation, Object[] keys, PointerPair[] values, PointerPair[] oldValues) {}
    private static class Snapshot{
        private List<Operation> operations;
        private Snapshot(List<Operation> operations){
            this.operations = operations;
        }
    }
    private List<Snapshot> snapshots = new ArrayList<>();
    public IndexSnapshot(){
        this.snapshots.add(new Snapshot(new ArrayList<>()));
    }
    public void addOperation(Operation operation){
        this.snapshots.getLast().operations.add(operation);
    }
    public void beginTransaction(){
        this.snapshots.add(new Snapshot(new ArrayList<>()));
    }
    public void commit(){
        Snapshot committed = this.snapshots.removeLast();
        if(this.snapshots.isEmpty()) {
            this.snapshots.add(new Snapshot(new ArrayList<>()));
            return;
        }
        this.snapshots.getLast().operations = new ArrayList<>(committed.operations);
    }
    public <K extends Comparable<? super K>> void rollback(BTreeSerialization<?>[] indexes){
        Snapshot removed = this.snapshots.removeLast();
        List<Operation> operations = removed.operations;
        for(int i = operations.size()-1;i>=0;i--){
            Operation operation = operations.get(i);
            for(int j = 0;j<indexes.length;j++){
                if(indexes[j] == null) continue;
                BTreeSerialization<K> index = (BTreeSerialization<K>)indexes[j];
                K key = (K)operation.keys[j];
                PointerPair value = operation.values[j];
                switch (operation.operation) {
                    case INSERT -> {
                        index.remove(key, value);
                    }
                    case REMOVE -> {
                        index.insert(key, value);
                    }
                    case UPDATE -> {
                        PointerPair oldValue = operation.oldValues[j];
                        if(index.isUnique()) index.update(key, oldValue);
                        else index.update(key, oldValue, value);
                    }
                }
            }
        }
        if(this.snapshots.isEmpty()) snapshots.add(new Snapshot(new ArrayList<>()));
    }
}
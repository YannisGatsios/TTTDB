package com.database.db.core.manager;

import com.database.db.api.Condition;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.ConditionUtils;
import com.database.db.core.index.*;
import com.database.db.core.index.IndexInit.PointerPair;
import com.database.db.core.manager.IndexManager.IndexRecord;
import com.database.db.core.page.Entry;
import com.database.db.core.table.DataType;
import com.database.db.core.table.Table;
import java.util.*;

public final class IndexUtils {

    private IndexUtils() {} // prevent instantiation

    @SuppressWarnings("unchecked")
    public static <K extends Comparable<? super K>> List<IndexRecord<K>> evaluateCondition(Table table, IndexInit<?>[] indexes, Condition<WhereClause> condition) {
        ConditionUtils.RangeBounds bounds = ConditionUtils.extractRange(condition.getConditions());
        int columnIndex = table.getSchema().getColumnIndex(condition.getColumnName());
        if (columnIndex < 0) return Collections.emptyList();
        IndexInit<K> index = (IndexInit<K>) indexes[columnIndex];
        List<Pair<K, PointerPair>> pairs =
            (index == null)
                ? SequentialOperations.sequentialRangeSearch(table, (K) bounds.start(), (K) bounds.end(), columnIndex)
                : index.rangeSearch((K) bounds.start(), (K) bounds.end());

        List<IndexRecord<K>> results = new ArrayList<>(pairs.size());
        for (Pair<K, PointerPair> p : pairs)
            if (condition.isApplicable(p.key))
                results.add(new IndexRecord<>(p.key, p.value, columnIndex));
        return results;
    }
    @SuppressWarnings("unchecked")
    public static <K extends Comparable<? super K>> List<IndexManager.IndexRecord<K>> noCondition(
        Table table, IndexInit<?>[] indexes, int columnIndex) {

        IndexInit<K> index = (IndexInit<K>) indexes[columnIndex];
        List<Pair<K, PointerPair>> pairs =
            (index == null)
                ? SequentialOperations.sequentialRangeSearch(table, null, null, columnIndex)
                : index.rangeSearch(null, null);

        List<IndexManager.IndexRecord<K>> result = pairs.isEmpty()
            ? Collections.emptyList()
            : new ArrayList<>(pairs.size());
        for (Pair<K, PointerPair> pair : pairs)
            result.add(new IndexManager.IndexRecord<>(pair.key, pair.value, columnIndex));
        return result;
    }

    public static <K> List<IndexManager.IndexRecord<K>> mergeOr(
            List<IndexManager.IndexRecord<K>> a, List<IndexManager.IndexRecord<K>> b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        List<IndexManager.IndexRecord<K>> merged = new ArrayList<>(a.size() + b.size());
        merged.addAll(a);
        for (IndexManager.IndexRecord<K> r : b)
            if (!a.contains(r)) merged.add(r);
        return merged;
    }

    public static <K> List<IndexManager.IndexRecord<K>> mergeAnd(
            List<IndexManager.IndexRecord<K>> a, List<IndexManager.IndexRecord<K>> b) {
        if (a.isEmpty() || b.isEmpty()) return Collections.emptyList();
        List<IndexManager.IndexRecord<K>> filtered = new ArrayList<>(a);
        filtered.retainAll(b);
        return filtered;
    }
    @SuppressWarnings("unchecked")
    public static <K extends Comparable<? super K>> K getValidatedKey(Entry entry, IndexInit<?> index, int columnIndex, DataType type) {
        K key = (K)entry.get(columnIndex);
        if (key == null) {
            if (!index.isNullable()) throw new IllegalArgumentException("Null key not allowed at column " + columnIndex);
            return null;
        }
        if (!type.getJavaClass().isInstance(key)) {
            throw new IllegalArgumentException(
                "Invalid secondary key type at column " + columnIndex +
                ": expected " + type.getJavaClass().getName() +
                ", but got " + key.getClass().getName()
            );
        }
        return key;
    }
}
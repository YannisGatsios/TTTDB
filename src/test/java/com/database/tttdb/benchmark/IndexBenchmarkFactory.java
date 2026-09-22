package com.database.tttdb.benchmark;

import com.database.tttdb.core.index.Index;
import com.database.tttdb.core.index.IndexInit.IndexType;
import com.database.tttdb.core.index.btree.BPlusTree;
import com.database.tttdb.core.index.hashmap.HashIndex;
import com.database.tttdb.core.index.redBlackTreeIndex.RedBlackTreeIndex;
import com.database.tttdb.core.index.skiplist.SkipListIndex;

final class IndexBenchmarkFactory {
    private IndexBenchmarkFactory() {}

    static Index<Integer, Integer> create(IndexType type, boolean unique) {
        Index<Integer, Integer> index = switch (type) {
            case BTREE -> new BPlusTree<>(64);
            case HASH_INDEX -> new HashIndex<>();
            case RED_BLACK_TREE -> new RedBlackTreeIndex<>();
            case SKIPLIST -> new SkipListIndex<>();
        };
        index.setUnique(unique);
        index.setNullable(false);
        return index;
    }
}

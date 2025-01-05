package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.List;

public class Node<K,V> {
    boolean isLeaf;
    List<Pair<K,V>> keys;
    List<Node<K,V>> children;
    Node<K,V> next;

    public Node(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.children = isLeaf ? null : new ArrayList<>();
        this.next = null;
    }
}

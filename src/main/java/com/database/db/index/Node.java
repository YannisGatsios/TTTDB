package com.database.db.index;

import java.util.ArrayList;
import java.util.List;

public class Node<K,V> {
    boolean isLeaf;
    public List<Pair<K,V>> pairs;
    public List<Node<K,V>> children;
    public Node<K,V> next;
    public Node<K,V> father;
    public Node<K,V> previous;

    public Node(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.pairs = new ArrayList<>();
        this.children = isLeaf ? null : new ArrayList<>();
        this.next = null;
    }

    @Override
    public String toString() {
        StringBuilder keys = new StringBuilder("| ");
        for(Pair<K,V> pair : this.pairs){
            keys.append(pair.toString()).append(" | ");
        }
        String border = "+"+"=".repeat(keys.length()-1)+"+";
        return "\n"+border+"\n"+keys+"\n"+border+"\n";
    }
}

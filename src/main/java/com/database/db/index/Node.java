package com.database.db.index;

import java.util.ArrayList;
import java.util.List;

public class Node<K,V> {
    boolean isLeaf;
    List<Pair<K,V>> pairs;
    List<Node<K,V>> children;
    Node<K,V> next;

    public Node(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.pairs = new ArrayList<>();
        this.children = isLeaf ? null : new ArrayList<>();
        this.next = null;
    }

    @Override
    public String toString() {
        String keys = "|";
        for(Pair<K,V> pair : this.pairs){
            keys += pair.toString();
        }
        String border = "+"+String.valueOf("-").repeat(keys.length()-1)+"+";
        return "\n"+border+"\n"+
                keys+"|\n"+
                border+"\n";
    }
}

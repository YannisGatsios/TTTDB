package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.List;

public class Node {
    boolean isLeaf;
    List<byte[]> keys;
    List<Node> children;
    Node next;

    public Node(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<byte[]>();
        this.children = new ArrayList<Node>();
        this.next = null;
    }
}

package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.List;
import com.database.db.bPlusTree.TreeUtils.Pair;;

public class Node {
    boolean isLeaf;
    List<Pair<byte[] ,Integer>> keys;
    List<Node> children;
    Node next;

    public Node(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<Pair<byte[] ,Integer>>();
        this.children = new ArrayList<Node>();
        this.next = null;
    }
}

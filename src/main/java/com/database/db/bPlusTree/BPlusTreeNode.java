package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode {
    boolean isLeaf;
    List<byte[]> keys;
    List<BPlusTreeNode> children;
    BPlusTreeNode next;

    public BPlusTreeNode(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<byte[]>();
        this.children = new ArrayList<BPlusTreeNode>();
        this.next = null;
    }
}

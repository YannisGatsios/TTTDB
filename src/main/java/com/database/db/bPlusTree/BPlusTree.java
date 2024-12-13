package com.database.db.bPlusTree;

import java.util.Comparator;
import java.util.stream.Collectors;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class BPlusTree {
    private BPlusTreeNode root;
    private final int order;
    private final Comparator<byte[]> keyComparator = (a, b) -> {
        for(int i = 0; i < Math.min(a.length, b.length); i++){
            int cmp = Byte.compare(a[i],b[i]);
            if(cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    };

    public BPlusTree(int order){
        if(order < 3){
            throw new IllegalArgumentException("BPlus Tree Order must be at least 3.");
        }
        this.root = new BPlusTreeNode(true);
        this.order = order;
    }

    public void insert(byte[] key){
        BPlusTreeNode leaf = this.findLeaf(key);
        this.insertIntoLeaf(leaf, key);
        if(leaf.keys.size() > this.order -1){
            splitLeaf(leaf);
        }
    }

    private BPlusTreeNode findLeaf(byte[] key) {
        BPlusTreeNode node = this.root;
    
        while (!node.isLeaf) {
            int i = 0;
            while (i < node.keys.size() && keyComparator.compare(key, node.keys.get(i)) >= 0) {
                i++;
            }
            // Move to the chosen child node
            node = node.children.get(i);
        }
        return node; // Return the leaf node
    }

    private void insertIntoLeaf(BPlusTreeNode leaf, byte[] key){
        int pos = Collections.binarySearch(leaf.keys, key, keyComparator);
        if(pos < 0){
            pos = -(pos + 1);
        }
        leaf.keys.add(pos, key);
    }

    private void splitLeaf(BPlusTreeNode leaf) {
        int mid = (this.order + 1) / 2;
        BPlusTreeNode newLeaf = new BPlusTreeNode(true);
    
        newLeaf.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
        leaf.keys.subList(mid, leaf.keys.size()).clear();
    
        // Set the next pointer correctly after splitting
        newLeaf.next = leaf.next;
        leaf.next = newLeaf;
    
        // If leaf is the root, create a new root
        if (leaf == root) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false);
            newRoot.keys.add(newLeaf.keys.get(0));
            newRoot.children.add(leaf);
            newRoot.children.add(newLeaf);
            this.root = newRoot;
        } else {
            insertIntoParent(leaf, newLeaf, newLeaf.keys.get(0));
        }
    }
    

    private void insertIntoParent(BPlusTreeNode left, BPlusTreeNode right, byte[] key){
        BPlusTreeNode parent = this.findParent(this.root, left);
        if(parent == null){
            throw new RuntimeException("Parent node not found for insertion.");
        }
        int pos = Collections.binarySearch(parent.keys, key, keyComparator);
        if(pos < 0){
            pos = -(pos + 1);
        }

        parent.keys.add(pos, key);
        parent.children.add(pos + 1, right);

        if(parent.keys.size() > this.order - 1){
            splitInternal(parent);
        }
    }

    private void splitInternal(BPlusTreeNode internal) {
        int mid = (this.order + 1) / 2;
        BPlusTreeNode newInternal = new BPlusTreeNode(false);
    
        newInternal.keys.addAll(internal.keys.subList(mid + 1, internal.keys.size()));
        internal.keys.subList(mid, internal.keys.size()).clear();
    
        newInternal.children.addAll(internal.children.subList(mid + 1, internal.children.size()));
        internal.children.subList(mid + 1, internal.children.size()).clear();
    
        // If internal is the root, create a new root
        if (internal == this.root) {
            BPlusTreeNode newRoot = new BPlusTreeNode(false);
            newRoot.keys.add(internal.keys.get(mid));
            newRoot.children.add(internal);
            newRoot.children.add(newInternal);
            this.root = newRoot;
        } else {
            this.insertIntoParent(internal, newInternal, internal.keys.remove(mid));
        }
    }
    

    private BPlusTreeNode findParent(BPlusTreeNode current, BPlusTreeNode target){
        if(current.isLeaf || current.children.isEmpty()){
            return null;
        }

        for(int i = 0; i < current.children.size(); i++){
            BPlusTreeNode child = current.children.get(i);
            if (child == target){
                return current;
            }

            BPlusTreeNode possibleParent = this.findParent(child, target);
            if(possibleParent != null){
                return possibleParent;
            }
        }
        return null;
    }
    public boolean search(byte[] key){
        BPlusTreeNode node = findLeaf(key);
        int pos = Collections.binarySearch(node.keys, key, this.keyComparator);
        return pos >= 0;
    }

    public void remove(byte[] key){
        BPlusTreeNode leaf = this.findLeaf(key);
        int pos = Collections.binarySearch(leaf.keys, key, keyComparator);
        if(pos >= 0){
            leaf.keys.remove(pos);
            if(leaf.keys.size() < (this.order - 1) / 2 && leaf != root){
                this.handleUnderFlow(leaf);
            }
        }
    }

    private void handleUnderFlow(BPlusTreeNode node){
        BPlusTreeNode parent = this.findParent(root, node);

        int index = parent.children.indexOf(node);
        BPlusTreeNode leftSliding = index > 0 ? parent.children.get(index - 1) : null;
        BPlusTreeNode rightSliding = index < parent.children.size() - 1 ? parent.children.get(index + 1) : null;
        if(leftSliding != null && leftSliding.keys.size() > (this.order -1) / 2){

        }else if(rightSliding != null && rightSliding.keys.size() > (this.order -1) / 2){

        }else{

        }
    }
    
    public void printTree() {
        printNode(root, 0);
    }

    private void printNode(BPlusTreeNode node, int level) {
        System.out.println("Level " + level + ": " + node.keys);
        if (!node.isLeaf) {
            for (BPlusTreeNode child : node.children) {
                printNode(child, level + 1);
            }
        }
    }
}

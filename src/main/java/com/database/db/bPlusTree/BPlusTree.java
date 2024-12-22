package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class BPlusTree {
    private Node root;
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
        this.root = null;
        this.order = order;
    }




    //========! INSERTION !==========
    public void insert(byte[] key){
        if(this.root == null){
            this.root = new Node(true);
            this.root.keys.add(key);
        }else{
            if(this.root.keys.size() == 2 * this.order - 1){
                Node newRoot = new Node(false);
                newRoot.children.add(this.root);
                this.splitChild(newRoot, 0, this.root);
                this.root = newRoot;
            }
            this.insertNonFull(root, key);
        }
    }

    private void splitChild(Node parent, int index, Node child){
        Node newChild = new Node(child.isLeaf);

        parent.children.add(index + 1,newChild);
        parent.keys.add(index, child.keys.get(this.order-1));

        newChild.keys.addAll(child.keys.subList(this.order, child.keys.size()));
        child.keys.subList(this.order - 1, child.keys.size()).clear();

        if(!child.isLeaf){
            newChild.children.addAll(child.children.subList(this.order, child.children.size()));
            child.children.subList(this.order, child.children.size()).clear();
        }

        if(child.isLeaf){
            newChild.next = child.next;
            child.next = newChild;
        }
    }

    private void insertNonFull(Node node, byte[] key){
        if (node.isLeaf){
            int pos = Collections.binarySearch(node.keys, key, keyComparator);
            if(pos < 0) pos = -(pos+1);
            node.keys.add(pos, key);
        }else{
            int i = node.keys.size() - 1;
            while (i >= 0 && Arrays.compare(key, node.keys.get(i)) < 0) {
                i--;
            }
            i++;
            if (node.children.get(i).keys.size() == 2*this.order - 1){
                this.splitChild(node, i, node.children.get(i));
                if(Arrays.compare(key, node.keys.get(i)) > 0){
                    i++;
                }
            }
            this.insertNonFull(node.children.get(i), key);
        }
    }

    //===========! REMOVING !=============
    public void remove(byte[] key){
        if(root == null){
            return;
        }
        this.remove(this.root, key);
        if(root.keys.isEmpty() && !this.root.isLeaf){
            this.root.children.get(0);
        }
    }

    private void remove(Node node, byte[] key){
        if(node.isLeaf){
            node.keys.remove(key);
        }else{
            int idx = Collections.binarySearch(node.keys, key, keyComparator);
            if(idx < 0) idx = -(idx + 1);

            if(idx < node.keys.size() && key.equals(node.keys.get(idx))){
                if(node.children.get(idx).keys.size() >= this.order){
                    Node predNode = node.children.get(idx);
                    while ((!predNode.isLeaf)) {
                        predNode = predNode.children.get(predNode.keys.size() - 1);
                    }
                    byte[] pred = predNode.keys.get(predNode.keys.size() - 1);
                    node.keys.set(idx, pred);
                    this.remove(node.children.get(idx), pred);
                }else if(node.children.get(idx + 1).keys.size() >= this.order){
                    Node succNode = node.children.get(idx + 1);
                    while (!succNode.isLeaf) {
                        succNode = succNode.children.get(0);
                    }
                    byte[] succ = succNode.keys.get(0);
                    node.keys.set(idx, succ);
                    this.remove(node.children.get(idx + 1), succ);
                }else{
                    this.merge(node, idx);
                    this.remove(node.children.get(idx), key);
                }
            }else{
                Node child = node.children.get(idx);
                if(child.keys.size() < this.order){
                    if(idx > 0 && node.children.get(idx - 1).keys.size() >= this.order){
                        this.borrowFromPrev(node, idx);
                    }else if(idx < node.children.size() -1 && node.children.get(idx + 1).keys.size() >= this.order){
                        this.borrowFromNext(node, idx);
                    }else{
                        if(idx < node.children.size() - 1){
                            this.merge(node, idx);
                        }else{
                            this.merge(node, idx - 1);
                        }
                    }
                }
                this.remove(child, key);
            }
        }
    }

    private void borrowFromPrev(Node node, int index){
        Node child = node.children.get(index);
        Node sibling = node.children.get(index - 1);

        child.keys.add(0, node.keys.get(index - 1));
        node.keys.set(index - 1, sibling.keys.remove(sibling.keys.size() - 1));

        if(!child.isLeaf){
            child.children.add(0, sibling.children.remove(sibling.children.size() -1));
        }
    }

    private void borrowFromNext(Node node, int index){
        Node child = node.children.get(index);
        Node sibling = node.children.get(index + 1);

        child.keys.add(node.keys.get(index));
        node.keys.set(index, sibling.keys.remove(0));

        if(!child.isLeaf){
            child.children.add(sibling.children.remove(0));
        }
    }

    private void merge(Node node, int index){
        Node child = node.children.get(index);
        Node sibling = node.children.get(index +1);

        child.keys.add(node.keys.remove(index));
        child.keys.addAll(sibling.keys);

        if(!child.isLeaf){
            child.children.addAll(sibling.children);
        }
        node.children.remove(index + 1);
    }

    //==========! SEARCHING !===========
    public boolean search(byte[] key){
        Node current = this.root;
        while (current != null) {
            int idx = Collections.binarySearch(current.keys, key, keyComparator);
            if(idx >= 0){
                return true;
            }
            idx = -(idx + 1);
            if(current.isLeaf){
                return false;
            }
            current =current.children.get(idx);
        }
        return false;
    }

    public List<byte[]> rangeQuery(byte[] lower, byte[] upper){
        List<byte[]> result = new ArrayList<>();
        Node current = root;
        while (current != null && !current.isLeaf) {
            int idx = 0;
            while (idx < current.keys.size() && Arrays.compare(lower, current.keys.get(idx)) > 0) {
                idx++;
            }
            current = current.children.get(idx);
        }
        while (current != null) {
            for(byte[] key : current.keys){
                if(Arrays.compare(key, lower) >= 0 && Arrays.compare(key, upper) <= 0){
                    result.add(key);
                }
                if(Arrays.compare(key, upper) > 0){
                    return result;
                }
            }
            current = current.next;
        }
        return result;
    }

    //===! PRINTING !====
    public void printTree() {
        printTree(root, 0);
    }

    private void printTree(Node node, int level) {
        if (node != null) {
            for (int i = 0; i < level; i++) {
                System.out.print("  ");
            }
            for (byte[] key : node.keys) {
                System.out.print(key + " ");
            }
            System.out.println();
            for (Node child : node.children) {
                printTree(child, level + 1);
            }
        }
    }
}

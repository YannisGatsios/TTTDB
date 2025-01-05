package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Tree<K extends Comparable<K>,V> extends BPlusTree<K,V>{

    private Node<K,V> root;
    private final int order;
    private int lastPageID;
    private final Comparator<Pair<K, V>> keyComparator = (pair1, pair2) -> {
        K key1 = pair1.getKey();
        K key2 = pair2.getKey();
        // Compare based on key types
        if (key1 instanceof Comparable) {
            return ((Comparable<K>) key1).compareTo(key2);
        }
        throw new IllegalArgumentException("Keys must implement Comparable.");
    };

    public Tree(int order){
        if(order < 3){
            throw new IllegalArgumentException("BPlus Tree Order must be at least 3.");
        }
        this.root = null;
        this.order = order;
        this.lastPageID = 0;
    }

    //========! INSERTION !==========
    public void insert(K key, V value){
        Pair<K,V> newPair = new Pair<K,V>(key, value);
        if(this.root == null){
            this.root = new Node<>(true);
            this.root.keys.add(newPair);
        }else{
            if(this.root.keys.size() == this.order){
                Node<K,V> newRoot = new Node<>(false);
                newRoot.children.add(this.root);
                this.splitChild(newRoot, 0, this.root);
                this.root = newRoot;
            }
            this.insertNonFull(root, newPair);
        }
    }

    private void splitChild(Node<K,V> parent, int index, Node<K,V> LChild){
        //RChild and LChild correspond to right or left child.
        Node<K,V> RChild = new Node<>(LChild.isLeaf);

        Pair<K,V> keyForParent = LChild.keys.get(this.order / 2);

        parent.children.add(index + 1,RChild);
        parent.keys.add(index, keyForParent);

        RChild.keys.addAll(LChild.keys.subList(this.order / 2 + 1, LChild.keys.size()));
        LChild.keys.subList(this.order / 2 + 1, LChild.keys.size()).clear();

        if(!LChild.isLeaf){
            RChild.children.addAll(LChild.children.subList(this.order / 2 + 1, LChild.children.size()));
            LChild.children.subList(this.order / 2 +1, LChild.children.size()).clear();
        }

        if(LChild.isLeaf){
            RChild.next = LChild.next;
            LChild.next = RChild;
        }
    }

    private void insertNonFull(Node<K,V> node, Pair<K,V> key){
        if (node.isLeaf){
            int pos = Collections.binarySearch(node.keys, key, keyComparator);
            if(pos < 0) pos = -(pos+1);
            node.keys.add(pos, key);
        }else{
            int i = node.keys.size() - 1;
            while (i >= 0 && keyComparator.compare(key, node.keys.get(i)) < 0) {
                i--;
            }
            i++;
            if (node.children.get(i).keys.size() == this.order){
                this.splitChild(node, i, node.children.get(i));
                if(keyComparator.compare(key, node.keys.get(i)) > 0){
                    i++;
                }
            }
            this.insertNonFull(node.children.get(i), key);
        }
    }

    //===========! REMOVING !=============
    public void remove(K key){
        if(root == null){
            return;
        }
        Pair<K,V> tempPair = new Pair<>(key, null);
        this.remove(this.root, tempPair);
        if(root.keys.isEmpty() && !this.root.isLeaf){
            Pair<K,V> lastKeyOfChild = this.root.children.get(0).keys.get(this.root.children.get(0).keys.size()-1);
            this.root.keys.add(lastKeyOfChild);
        }
    }

    private void remove(Node<K,V> node, Pair<K,V> key) {
        int idx = Collections.binarySearch(node.keys, key, keyComparator);
        if (idx >= 0) {
            if (node.isLeaf) {
                // Key found in leaf node
                node.keys.remove(idx);
            } else {
                // Key found in internal node
                Node<K,V> predNode = node.children.get(idx);
                if (predNode.keys.size() >= (order + 1) / 2) {
                    Pair<K,V> pred = getPredecessor(predNode);
                    node.keys.set(idx, pred);
                    remove(predNode, pred);
                } else if (node.children.get(idx + 1).keys.size() >= (order + 1) / 2) {
                    Node<K,V> succNode = node.children.get(idx + 1);
                    Pair<K,V> succ = getSuccessor(succNode);
                    node.keys.set(idx, succ);
                    remove(succNode, succ);
                } else {
                    merge(node, idx);
                    remove(node.children.get(idx), key);
                }
            }
        } else {
            // Key not found in current node
            idx = -(idx + 1);
            if (node.children.get(idx).keys.size() < (order + 1) / 2) {
                if (idx > 0 && node.children.get(idx - 1).keys.size() >= (order + 1) / 2) {
                    borrowFromPrev(node, idx);
                } else if (idx < node.children.size() - 1 && node.children.get(idx + 1).keys.size() >= (order + 1) / 2) {
                    borrowFromNext(node, idx);
                } else {
                    if (idx < node.children.size() - 1) {
                        merge(node, idx);
                    } else {
                        merge(node, idx - 1);
                    }
                }
            }
            remove(node.children.get(idx), key);
        }
    }
    
    private Pair<K,V> getPredecessor(Node<K,V> node) {
        while (!node.isLeaf) {
            node = node.children.get(node.children.size() - 1);
        }
        return node.keys.get(node.keys.size() - 1);
    }
    
    private Pair<K,V> getSuccessor(Node<K,V> node) {
        while (!node.isLeaf) {
            node = node.children.get(0);
        }
        return node.keys.get(0);
    }

    private void borrowFromPrev(Node<K,V> node, int index){
        Node<K,V> child = node.children.get(index);
        Node<K,V> sibling = node.children.get(index - 1);

        child.keys.add(0, node.keys.get(index - 1));
        node.keys.set(index - 1, sibling.keys.remove(sibling.keys.size() - 1));

        if(!child.isLeaf){
            child.children.add(0, sibling.children.remove(sibling.children.size() -1));
        }
    }

    private void borrowFromNext(Node<K,V> node, int index){
        Node<K,V> child = node.children.get(index);
        Node<K,V> sibling = node.children.get(index + 1);

        child.keys.add(node.keys.get(index));
        node.keys.set(index, sibling.keys.remove(0));

        if(!child.isLeaf){
            child.children.add(sibling.children.remove(0));
        }
    }

    private void merge(Node<K,V> node, int index){
        Node<K,V> child = node.children.get(index);
        Node<K,V> sibling = node.children.get(index +1);

        child.keys.add(node.keys.remove(index));
        child.keys.addAll(sibling.keys);

        if(!child.isLeaf){
            child.children.addAll(sibling.children);
        }
        node.children.remove(index + 1);
        if(child.isLeaf){
            child.next = sibling.next;
        }
    }

    //==========! SEARCHING !===========
    public boolean isKey(K key) {
        if(this.root == null) return false;
        Pair<K,V> tempPair = new Pair<>(key, null);
        Node<K,V> current = this.root;
        // Traverse down the tree to find the leaf node
        while (current != null) {
            int idx = Collections.binarySearch(current.keys, tempPair, keyComparator);
            if (idx >= 0) {
                // If the key is found in the current node, we check if it's a leaf node
                if (current.isLeaf) {
                    return true;  // Key found in a leaf node
                } else {
                    // If not a leaf, continue to the appropriate child
                    current = current.children.get(idx);
                }
            } else {
                // Key not found in the current node, determine which child to go to
                idx = -(idx + 1);
                if (current.isLeaf) {
                    return false;  // Key is not found in a leaf node
                }
                current = current.children.get(idx);
            }
        }
        return false;
    }

    public V search(K key) {
        Pair<K,V> tempPair = new Pair<>(key, null);
        Node<K,V> current = this.root;
        // Traverse down the tree to find the leaf node
        while (current != null) {
            int idx = Collections.binarySearch(current.keys, tempPair, keyComparator);
            if (idx >= 0) {
                // If the key is found in the current node, we check if it's a leaf node
                if (current.isLeaf) {
                    return current.keys.get(idx).getValue();  // Key found in a leaf node
                } else {
                    // If not a leaf, continue to the appropriate child
                    current = current.children.get(idx);
                }
            } else {
                // Key not found in the current node, determine which child to go to
                idx = -(idx + 1);
                if (current.isLeaf) {
                    return null;  // Key is not found in a leaf node
                }
                current = current.children.get(idx);
            }
        }
        return null;
    }
    

    public List<Pair<K,V>> rangeSearch( K lower, K upper){
        List<Pair<K,V>> result = new ArrayList<>();
        Pair<K,V> tempLow = new Pair<>(lower, null);
        Pair<K,V> tempUp = new Pair<>(upper, null);
        Node<K,V> current = root;
        while (current != null && !current.isLeaf) {
            int idx = 0;
            while (idx < current.keys.size() && keyComparator.compare(tempLow, current.keys.get(idx)) > 0) {
                idx++;
            }
            current = current.children.get(idx);
        }
        while (current != null) {
            for(Pair<K,V> key : current.keys){
                if(keyComparator.compare(key, tempLow) >= 0 && keyComparator.compare(key, tempUp) <= 0){
                    result.add(key);
                }
                if(keyComparator.compare(key, tempUp) > 0){
                    return result;
                }
            }
            current = current.next;
        }
        return result;
    }

    //=======! PRINTING !======
    public void printTree(){
        List<List<String>> tree = new ArrayList<List<String>>();
        tree.add(0 ,new ArrayList<>(List.of(this.printNode(this.root))));
        tree = this.printTree(this.root, 1, tree);
        for (int i = 0; i < tree.size(); i++) {
            System.out.println("!=========! Level " + i + " !=========!\n" + tree.get(i));
        }
        for (int i = 0; i < tree.size(); i++) {
            System.out.println("!=========! Level " + i + " !=========!\nNum Of Nodes : " + tree.get(i).size());
        }
    }
    private List<List<String>> printTree(Node<K,V> node, int level, List<List<String>> tree){
        if(!node.isLeaf){
            if (tree.size() <= level) tree.add(new ArrayList<>());
            for(int i = 0;i < node.children.size();i++){
                Node<K,V> curChild = node.children.get(i);
                tree.get(level).add(this.printNode(curChild));
                if(!curChild.isLeaf){
                    tree = this.printTree(curChild, level+1, tree);
                }
            }
        }
        return tree;
    }
    private String printNode(Node<K,V> node){
        String keys = "|";
        for(int i = 0; i < node.keys.size();i++){
            keys += "  "+ this.byteArrayToString(node.keys.get(i).getKey())+" : "+node.keys.get(i).getValue()+"  |";
        }
        String border = "+"+String.valueOf("-").repeat(keys.length()-2)+"+";
        return "\n"+border+"\n"+
                keys+"\n"+
                border+"\n";
    }
    private String byteArrayToString(Object byteArray) {
        if(byteArray instanceof byte[]){
            StringBuilder sb = new StringBuilder();
            for (byte b : (byte[])byteArray) {
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(b & 0xFF);
            }
            return sb.toString();
        }
        return byteArray.toString();
    }

    public Node<K,V> getRoot(){
        return this.root;
    }
    public void setLastPageID(int lastPageID){
        this.lastPageID = lastPageID;
    }public int getLastPageID(){
        return this.lastPageID;
    }public void addOnePageID(){
        this.lastPageID = this.lastPageID + 1;
    }public void removeOnePageID(){
        this.lastPageID = this.lastPageID - 1;
    }

}
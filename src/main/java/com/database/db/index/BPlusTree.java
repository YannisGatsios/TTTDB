package com.database.db.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.database.db.table.Table;

public abstract class BPlusTree<K extends Comparable<K>,V> implements BTree<K,V>  {

    private Node<K,V> root;
    private final int order;
    private int numberOfPages;
    private int numOfEntries;//Used for Auto Incrementing Columns.
    private final Comparator<Pair<K, V>> keyComparator = (pair1, pair2) -> {
        K key1 = pair1.key;
        K key2 = pair2.key;
        return key1.compareTo(key2);
    };

    public BPlusTree(int order){
        if(order < 3){
            throw new IllegalArgumentException("BPlus Tree Order must be at least 3.");
        }
        this.root = null;
        this.order = order;
        this.numOfEntries = 0;
        this.numberOfPages = 0;
    }

    //========! INSERTION !==========
    public void insert(K key, V value){
        Pair<K,V> newPair = new Pair<K,V>(key, value);
        if(this.root == null){
            this.root = new Node<>(true);
            this.root.pairs.add(newPair);
            this.numOfEntries++;//increment the number of entries in the tree.

        }else{
            if(this.root.pairs.size() == this.order-1){
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

        Pair<K,V> keyForParent = LChild.pairs.get(this.order / 2);

        parent.children.add(index + 1,RChild);
        parent.pairs.add(index, keyForParent);

        RChild.pairs.addAll(LChild.pairs.subList(this.order / 2 + 1, LChild.pairs.size()));
        LChild.pairs.subList(this.order / 2 + 1, LChild.pairs.size()).clear();

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
            int pos = Collections.binarySearch(node.pairs, key, keyComparator);
            if(pos < 0) pos = -(pos+1);
            node.pairs.add(pos, key);
            this.numOfEntries++;//increment the number of entries in the tree.

        }else{
            int i = node.pairs.size() - 1;
            while (i >= 0 && keyComparator.compare(key, node.pairs.get(i)) < 0) {
                i--;
            }
            i++;
            if (node.children.get(i).pairs.size() == this.order-1){
                this.splitChild(node, i, node.children.get(i));
                if(keyComparator.compare(key, node.pairs.get(i)) > 0){
                    i++;
                }
            }
            this.insertNonFull(node.children.get(i), key);
        }
    }

    //===========! REMOVING !=============
    public void remove(K key){
        if(root != null){
            Pair<K, V> tempPair = new Pair<>(key, null);
            this.remove(this.root, tempPair);
            if (root.pairs.isEmpty() && !this.root.isLeaf) {
                Pair<K, V> lastKeyOfChild = this.root.children.get(0).pairs
                        .get(this.root.children.get(0).pairs.size() - 1);
                this.root.pairs.add(lastKeyOfChild);
            }
        }
    }

    private void remove(Node<K,V> node, Pair<K,V> key) {
        int idx = Collections.binarySearch(node.pairs, key, keyComparator);
        if (idx >= 0) {// Key can be found in current Node.
            if (node.isLeaf) {// Current Node is leaf Node.
                node.pairs.remove(idx);
                this.numOfEntries--;
            } else {// Current Node is not leaf Node.
                Node<K,V> predecessorNode = node.children.get(idx);
                if (predecessorNode.pairs.size() >= (order + 1) / 2) {
                    Pair<K,V> predecessor = getPredecessor(predecessorNode);
                    node.pairs.set(idx, predecessor);
                    this.remove(predecessorNode, predecessor);
                } else if (idx+1 < node.children.size() && node.children.get(idx + 1).pairs.size() >= (order + 1) / 2) {
                    Node<K,V> successorNode = node.children.get(idx + 1);
                    Pair<K,V> successor = getSuccessor(successorNode);
                    node.pairs.set(idx, successor);
                    this.remove(successorNode, successor);
                } else {
                    this.merge(node, idx);
                    this.remove(node.children.get(idx), key);
                }
            }
        } else {// Key can not be found in current Node.
            idx = -(idx + 1);
            if (node.children.get(idx).pairs.size() < (order + 1) / 2) {
                if (idx > 0 && node.children.get(idx - 1).pairs.size() >= (order + 1) / 2) {
                    this.borrowFromPrev(node, idx);
                } else if (idx < node.children.size() - 1 && node.children.get(idx + 1).pairs.size() >= (order + 1) / 2) {
                    this.borrowFromNext(node, idx);
                } else {
                    if (idx < node.children.size() - 1) {
                        this.merge(node, idx);
                    } else {
                        this.merge(node, idx - 1);
                        idx = idx - 1;
                    }
                }
            }
            this.remove(node.children.get(idx), key);
        }
    }
    
    private Pair<K,V> getPredecessor(Node<K,V> node) {
        while (!node.isLeaf) {
            node = node.children.get(node.children.size() - 1);
        }
        return node.pairs.get(node.pairs.size() - 1);
    }
    
    private Pair<K,V> getSuccessor(Node<K,V> node) {
        while (!node.isLeaf) {
            node = node.children.get(0);
        }
        return node.pairs.get(0);
    }

    private void borrowFromPrev(Node<K,V> masterNode, int index){
        Node<K,V> child = masterNode.children.get(index);
        Node<K,V> sibling = masterNode.children.get(index - 1);

        child.pairs.add(0, masterNode.pairs.get(index - 1));
        masterNode.pairs.set(index - 1, sibling.pairs.remove(sibling.pairs.size() - 1));

        if(!child.isLeaf){
            child.children.add(0, sibling.children.remove(sibling.children.size() -1));
        }
    }

    private void borrowFromNext(Node<K,V> masterNode, int index){
        Node<K,V> child = masterNode.children.get(index);
        Node<K,V> sibling = masterNode.children.get(index + 1);

        child.pairs.add(masterNode.pairs.get(index));
        masterNode.pairs.set(index, sibling.pairs.remove(0));

        if(!child.isLeaf){
            child.children.add(sibling.children.remove(0));
        }
    }

    private void merge(Node<K,V> masterNode, int index){
        Node<K,V> child = masterNode.children.get(index);
        Node<K,V> sibling = masterNode.children.get(index + 1);

        child.pairs.add(masterNode.pairs.remove(index));
        child.pairs.addAll(sibling.pairs);

        if(!child.isLeaf){
            child.children.addAll(sibling.children);
        }
        masterNode.children.remove(index + 1);
        if(child.isLeaf){
            child.next = sibling.next;
        }
    }

    //==========! SEARCHING !===========
    public V search(K key) {
        Node<K,V> node = this.findNode(key);
        if(node == null) return null;
        int idx = Collections.binarySearch(node.pairs, new Pair<>(key, null), keyComparator);
        if(idx >= 0){
            return node.pairs.get(idx).value;
        }else{
            return null;
        }
    }

    public boolean isKey(K key) {
        if(this.root == null) return false;
        return this.search(key) != null;
    }

    private Node<K,V> findNode(K key){
        Pair<K, V> tempPair = new Pair<>(key, null);
        Node<K, V> current = this.root;
        // Traverse down the tree to find the leaf node that can contain the key.
        while (current != null) {
            int idx = Collections.binarySearch(current.pairs, tempPair, keyComparator);
            // checking if the key can not be present in the current node.
            if (idx < 0) {
                idx = -(idx + 1);
            }
            if (current.isLeaf) {
                return current;// Return the leaf node if key can be found.
            } else {
                current = current.children.get(idx);// If not a leaf Node, traverse to the appropriate child.
            }
        }
        return null;
    }

    public List<Pair<K,V>> rangeSearch( K lower, K upper){
        List<Pair<K,V>> result = new ArrayList<>();
        Pair<K,V> tempLow = new Pair<>(lower, null);
        Pair<K,V> tempUp = new Pair<>(upper, null);
        Node<K, V> current = this.findNode(lower);

        int idx = Collections.binarySearch(current.pairs, tempLow, keyComparator);
        if (idx < 0) {
            idx = -(idx + 1);
        }
        result.addAll(current.pairs.subList(idx, current.pairs.size()));
        current = current.next;
        
        while (current != null) {
            idx = Collections.binarySearch(current.pairs, tempUp, keyComparator);
            if (idx < 0) {
                result.addAll(current.pairs);
                current = current.next;
            }else{
                result.addAll(current.pairs.subList(0, idx + 1));
                current = null;
            }
        }
        return result;
    }

    // ===============UPDATE=============
    public void update(K key, V newValue) {
        if (key == null) {
            System.out.println("Update failed: Key is null.");
            return;
        }
        if (newValue == null) {
            System.out.println("Update failed: Value is null.");
            return;
        }
        if (!this.isKey(key)) {
            System.out.println("From Update key NOT FOUND on tree, Key : " + key.toString());
            return;
        }
        this.remove(key);
        this.insert(key, newValue);
        System.out.println("updated : [" + key.toString() + " : " + newValue.toString() + " --- " + this.search(key));
    }

    //=======! PRINTING !======
    @Override
    public String toString(){
        String result = "";
        List<List<String>> tree = new ArrayList<List<String>>();
        tree.add(0 ,new ArrayList<>(List.of(this.root.toString())));
        tree = this.printTree(this.root, 1, tree);
        for (int i = 0; i < tree.size(); i++) {
            result += "!=========! Level " + i + " !=========!\n" + tree.get(i)+"\n";
        }
        for (int i = 0; i < tree.size(); i++) {
            result += "!=========! Level " + i + " !=========!\nNum Of Nodes : " + tree.get(i).size()+"\n";
        }
        return result;
    }
    private List<List<String>> printTree(Node<K,V> node, int level, List<List<String>> tree){
        if(!node.isLeaf){
            if (tree.size() <= level) tree.add(new ArrayList<>());
            for(int i = 0;i < node.children.size();i++){
                Node<K,V> curChild = node.children.get(i);
                tree.get(level).add(curChild.toString());
                if(!curChild.isLeaf){
                    tree = this.printTree(curChild, level+1, tree);
                }
            }
        }
        return tree;
    }

    public Node<K,V> getRoot(){
        return this.root;
    }
    public void setNumberOfPages(int lastPageID){
        this.numberOfPages = lastPageID;
    }public int getNumberOfPages(){
        return this.numberOfPages;
    }public void addOnePage(){
        this.numberOfPages = this.numberOfPages + 1;
    }public void removeOnePage(){
        this.numberOfPages = this.numberOfPages - 1;
    }

    public abstract byte[] treeToBuffer(int maxSizeOfKey);
    public abstract BPlusTree<K, V> bufferToTree(byte[] treeBuffer, Table<K> table);
}
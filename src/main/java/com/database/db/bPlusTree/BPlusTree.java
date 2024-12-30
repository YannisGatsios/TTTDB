package com.database.db.bPlusTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class BPlusTree extends TreeUtils{

    private Node root;
    private final int order;
    private int lastPageID;
    private final Comparator<Pair<?, Integer>> keyComparator = (pair1, pair2) -> {
        Object key1 = pair1.getKey();
        Object key2 = pair2.getKey();
    
        // Ensure both keys are of the same type (String or Integer)
        if (key1.getClass() != key2.getClass()) {
            throw new IllegalArgumentException("Keys must be of the same type (String or Integer).");
        }
    
        if (key1 instanceof String && key2 instanceof String) {
            return ((String) key1).compareTo((String) key2);
        } else if (key1 instanceof Integer && key2 instanceof Integer) {
            return Integer.compare((Integer) key1, (Integer) key2);
        } else if (key1 instanceof byte[] && key2 instanceof byte[]) {
            byte[] bkey1 = (byte[]) key1;
            byte[] bkey2 = (byte[]) key2;
            for (int i = 0; i < Math.min(bkey1.length, bkey2.length); i++) {
                int cmp = bkey1[i] - bkey2[i];
                if (cmp != 0)
                    return cmp;
            }
            return Integer.compare(bkey1.length, bkey2.length);
        } else {
            throw new IllegalArgumentException("Unsupported key type. Only String and Integer are allowed.");
        }
    };

    public BPlusTree(int order){
        if(order < 3){
            throw new IllegalArgumentException("BPlus Tree Order must be at least 3.");
        }
        this.root = null;
        this.order = order;
        this.lastPageID = 0;
    }

    //========! INSERTION !==========
    public void insert(Pair<?, Integer> key){
        if(this.root == null){
            this.root = new Node(true);
            this.root.keys.add(key);
        }else{
            if(this.root.keys.size() == this.order){
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

        Pair<?, Integer> medKey = child.keys.get(this.order / 2);

        parent.children.add(index + 1,newChild);
        parent.keys.add(index, medKey);

        newChild.keys.addAll(child.keys.subList(this.order / 2 + 1, child.keys.size()));
        child.keys.subList(this.order / 2 + 1, child.keys.size()).clear();

        if(!child.isLeaf){
            newChild.children.addAll(child.children.subList(this.order / 2 + 1, child.children.size()));
            child.children.subList(this.order / 2 + 1, child.children.size()).clear();
        }

        if(child.isLeaf){
            newChild.next = child.next;
            child.next = newChild;
        }
    }

    private void insertNonFull(Node node, Pair<?, Integer> key){
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
    public void remove(Object key){
        if(root == null){
            return;
        }
        Pair<?, Integer> tempPair = new Pair<>(key, null);
        this.remove(this.root, tempPair);
        if(root.keys.isEmpty() && !this.root.isLeaf){
            Pair<?, Integer> lastKeyOfChild = this.root.children.get(0).keys.get(this.root.children.get(0).keys.size()-1);
            this.root.keys.add(lastKeyOfChild);
        }
    }

    private void remove(Node node, Pair<?, Integer> key) {
        int idx = Collections.binarySearch(node.keys, key, keyComparator);
        if (idx >= 0) {
            if (node.isLeaf) {
                // Key found in leaf node
                node.keys.remove(idx);
            } else {
                // Key found in internal node
                Node predNode = node.children.get(idx);
                if (predNode.keys.size() >= (order + 1) / 2) {
                    Pair<?, Integer> pred = getPredecessor(predNode);
                    node.keys.set(idx, pred);
                    remove(predNode, pred);
                } else if (node.children.get(idx + 1).keys.size() >= (order + 1) / 2) {
                    Node succNode = node.children.get(idx + 1);
                    Pair<?, Integer> succ = getSuccessor(succNode);
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
    
    private Pair<?, Integer> getPredecessor(Node node) {
        while (!node.isLeaf) {
            node = node.children.get(node.children.size() - 1);
        }
        return node.keys.get(node.keys.size() - 1);
    }
    
    private Pair<?, Integer> getSuccessor(Node node) {
        while (!node.isLeaf) {
            node = node.children.get(0);
        }
        return node.keys.get(0);
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
        if(child.isLeaf){
            child.next = sibling.next;
        }
    }

    //==========! SEARCHING !===========
    public boolean search(Object key) {
        if(this.root == null) return false;
        Pair<?, Integer> tempPair = new Pair<>(key, null);
        Node current = this.root;
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

    public Pair<?, Integer> findPair(Object key) {
        Pair<?, Integer> tempPair = new Pair<>(key, null);
        Node current = this.root;
        // Traverse down the tree to find the leaf node
        while (current != null) {
            int idx = Collections.binarySearch(current.keys, tempPair, keyComparator);
            if (idx >= 0) {
                // If the key is found in the current node, we check if it's a leaf node
                if (current.isLeaf) {
                    return current.keys.get(idx);  // Key found in a leaf node
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
    

    public List<Pair<?, Integer>> rangeQuery( Object lower, Object upper){
        List<Pair<?, Integer>> result = new ArrayList<>();
        Pair<?, Integer> tempLow = new Pair<>(lower, null);
        Pair<?, Integer> tempUp = new Pair<>(upper, null);
        Node current = root;
        while (current != null && !current.isLeaf) {
            int idx = 0;
            while (idx < current.keys.size() && keyComparator.compare(tempLow, current.keys.get(idx)) > 0) {
                idx++;
            }
            current = current.children.get(idx);
        }
        while (current != null) {
            for(Pair<?, Integer> key : current.keys){
                if(keyComparator.compare(key, tempLow) >= 0 && keyComparator.compare(key, tempUp) <= 0){
                    result.add(key);
                }
                if(keyComparator.compare(key, tempLow) > 0){
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
    private List<List<String>> printTree(Node node, int level, List<List<String>> tree){
        if(!node.isLeaf){
            if (tree.size() <= level) tree.add(new ArrayList<>());
            for(int i = 0;i < node.children.size();i++){
                Node curChild = node.children.get(i);
                tree.get(level).add(this.printNode(curChild));
                if(!curChild.isLeaf){
                    tree = this.printTree(curChild, level+1, tree);
                }
            }
        }
        return tree;
    }
    private String printNode(Node node){
        String keys = "|";
        for(int i = 0; i < node.keys.size();i++){
            keys += "  "+ this.byteArrayToString(node.keys.get(i).getKey())+" : "+node.keys.get(i).getValue()+"  |";
        }
        String border = "+"+String.valueOf("-").repeat(keys.length()-2)+"+";
        return "\n"+border+"\n"+
                keys+"\n"+
                border+"\n"+
                "Children : "+node.children.size()+"\n";
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

    public Node getRoot(){
        return this.root;
    }

    public int getLastPageID(){
        return this.lastPageID;
    }public void addOnePageID(){
        this.lastPageID = this.lastPageID + 1;
    }public void setLastPageID(int lastPageID){
        this.lastPageID = lastPageID;
    }
}
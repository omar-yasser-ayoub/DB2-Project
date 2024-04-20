package org.example.data_structures.index;

import org.example.data_structures.Tuple;

import java.io.Serializable;
import java.util.Vector;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
	private BTreeNode<TKey> root;

	public BTreeNode<TKey> getRoot() {
		return this.root;
	}
	
	public BTree() {
		this.root = new BTreeLeafNode<TKey, TValue>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		leaf.insertKey(key, value);
		
		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n; 
		}
	}
	
	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public TValue search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}
	
	/**
	 * Delete a key and its associated value from the tree.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n; 
		}
	}
	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}

		return (BTreeLeafNode<TKey, TValue>)node;
	}
	public Vector<String> equalSearch(TKey key){
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		Vector<String> result = new Vector<>();

		boolean found = false;
		while(leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				if (leaf.getKey(i).compareTo(key) == 0) {
					found = true;
					result.add(leaf.getValue(i).toString());
				}
				else {
					if (found) return result;
				}
			}
			leaf = (BTreeLeafNode<TKey, TValue>) leaf.getRightCousin();
		}
		return result;
	}
	public Vector<String> greaterThanSearch(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		Vector<String> result = new Vector<>();

		while(leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				if (leaf.getKey(i).compareTo(key) > 0) {
					result.add(leaf.getValue(i).toString());
				}
			}
			leaf = (BTreeLeafNode<TKey, TValue>) leaf.getRightCousin();
		}
		return result;
	}

	public Vector<String> greaterThanOrEqualSearch(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		Vector<String> result = new Vector<>();
		while(leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				result.add(leaf.getValue(i).toString());
			}
			leaf = (BTreeLeafNode<TKey, TValue>) leaf.getRightCousin();
		}
		return result;
	}

	public Vector<String> lessThanSearch(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		Vector<String> result = new Vector<>();

		while(leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				if (leaf.getKey(i).compareTo(key) < 0) {
					result.add(leaf.getValue(i).toString());
				}
			}
			leaf = (BTreeLeafNode<TKey, TValue>) leaf.getLeftCousin();
		}
		return result;
	}

	public Vector<String> lessThanOrEqualSearch(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		Vector<String> result = new Vector<>();

		while(leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				if (leaf.getKey(i).compareTo(key) <= 0) {
					result.add(leaf.getValue(i).toString());
				}
			}
			leaf = (BTreeLeafNode<TKey, TValue>) leaf.getLeftCousin();
		}
		return result;
	}
}

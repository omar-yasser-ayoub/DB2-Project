package org.example.data_structures.index;

import java.io.Serializable;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
	private BTreeNode<TKey> root;
	
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
	private void searchBigger(TKey value) {
		BTreeNode x = this.findLeafNodeShouldContainKey(value);
		while (x != null) {
			if (x instanceof BTreeInnerNode<?>) {
				System.out.println("problem");
			}
			else {
				for (int i = 0 ; i < 5; i++) {
					if (x.getKey(i) != null) {
						System.out.println(x.getKey(i));
					}
					else {
						break;
					}
				}
			}
			if (x.getRightSibling() == null) {
				while (x.getRightSibling() == null) {
					x = x.getParent();
					if (x == this.root) {
						return;
					}
				}
				x = x.getRightSibling();
				while (x instanceof BTreeInnerNode<?>) {
					x = ((BTreeInnerNode<?>) x).getChild(0);
				}
			}
			else {
				x = x.getRightSibling();
			}
		}
	}
	private void searchSmaller(TKey value) {
		BTreeNode x = this.findLeafNodeShouldContainKey(value);
		while (x != null) {
			if (x instanceof BTreeInnerNode<?>) {
				System.out.println("problem");
			} else {
				for (int i = 0; i < 5; i++) {
					if (x.getKey(i) != null) {
						if (x.getKey(i).compareTo(value) < 0) {
							System.out.println(x.getKey(i));
						} else {
							break; // No need to continue if we find a key greater than or equal to the given value
						}
					} else {
						break;
					}
				}
			}
			if (x.getLeftSibling() == null) {
				while (x.getLeftSibling() == null) {
					x = x.getParent();
					if (x == this.root) {
						return;
					}
				}
				x = x.getLeftSibling();
				while (x instanceof BTreeInnerNode<?>) {
					x = ((BTreeInnerNode<?>) x).getChild(((BTreeInnerNode<?>) x).getNumKeys());
				}
			} else {
				x = x.getLeftSibling();
			}
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

	public static void main(String[] args) throws Exception {
		BTree<Integer,Integer> test = new BTree<>();
		for (int i = 0; i < 50; i++) {
			test.insert(i , i);
		}
		BTreeLeafNode x = test.findLeafNodeShouldContainKey(0);

		System.out.println("printing my stuff");
		while (x != null) {
			System.out.println(x.getValue(0));
			System.out.println(x.getValue(1));
			x = (BTreeLeafNode) x.getRightSibling();
		}
	}
}

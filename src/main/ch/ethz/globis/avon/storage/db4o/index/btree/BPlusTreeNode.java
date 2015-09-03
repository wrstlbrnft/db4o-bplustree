/*
 * @(#)BTreeNode.java   1.0   Feb 16, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: BPlusTreeNode.java 4235 2009-07-09 14:15:43Z D\zimmerch $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.db4o.activation.ActivationPurpose;
import com.db4o.activation.Activator;
import com.db4o.ta.Activatable;


/**
 * This is a node in a B+ Tree that can hold keys, values associated with the keys and references to child nodes.
 * Leaf nodes in the tree don't have children.
 * Non-leaf nodes have children, but no values - they only contain keys for the navigation in the tree.
 * All data is stored in the leafs.
 *
 * @author Christoph Zimmerli &lt;zimmerch@ethz.ch&gt;
 * @version 1.0
 */
public class BPlusTreeNode<T extends Comparable<T>> implements Activatable {

   private static final boolean LEFT = true;
   private static final boolean RIGHT = false;

   private final int order;
   private final BPlusTreeKey<T>[] keys;
   private final List<Object>[] entries;
   private final BPlusTreeNode<T>[] children;
   private BPlusTreeNode<T> parent;
   private BPlusTreeNode<T> previous;
   private BPlusTreeNode<T> next;
   private int currentKeyCount;
   private final boolean isLeaf;
   private final int medianIndex;
   private transient Activator activator;

   public BPlusTreeNode(final int order, final boolean isLeaf) {
      this.parent = null;
      this.previous = null;
      this.next = null;
      this.order = order;
      this.currentKeyCount = 0;
      this.isLeaf = isLeaf;
      final int entryCount = 2 * this.order;
      this.keys = new BPlusTreeKey[entryCount];
      this.entries = new ArrayList[entryCount];
      for (int i = 0; i < entryCount; i++) {
         this.keys[i] = new BPlusTreeKey<T>();
         this.entries[i] = new ArrayList<Object>();
      }
      this.medianIndex = (entryCount - 1) / 2;
      this.children = new BPlusTreeNode[2 * this.order + 1];
   }

   private BPlusTreeNode(final BPlusTreeNode<T> parent, final int order,
         final boolean isLeaf, final BPlusTreeKey<T>[] keys, final List<Object>[] entries) {
      this(order, isLeaf);
      System.arraycopy(keys, 0, this.keys, 0, keys.length);
      System.arraycopy(entries, 0, this.entries, 0, entries.length);
      this.currentKeyCount = keys.length;
   }

   private BPlusTreeNode(final BPlusTreeNode<T> parent, final int order,
         final boolean isLeaf, final BPlusTreeKey<T>[] keys, final List<Object>[] entries,
         final BPlusTreeNode<T>[] children) {
      this(parent, order, isLeaf, keys, entries);
      System.arraycopy(children, 0, this.children, 0, children.length);
   }

   public List<Object> search(final T key) {
      this.activate(ActivationPurpose.READ);
      final int keyPosition = this.findKey(key);
      if (this.isLeaf) {
         if (keyPosition < this.currentKeyCount
               && this.keysEqual(this.keys[keyPosition].key(), key)) {
            return this.entries[keyPosition];
         } else {
            return new ArrayList<Object>();
         }
      } else {
         return this.children[keyPosition].search(key);
      }
   }

   private int findChild(final BPlusTreeNode<T> child, final int keyPosition) {
      // given the key position, the child should be either at keyPosition
      // or at keyPosition + 1.
      if (this.children[keyPosition] == child) {
         return keyPosition;
      }
      if (this.children[keyPosition + 1] == child) {
         return keyPosition + 1;
      }
      throw new RuntimeException("Child was not found at expected positions.");
   }

   private int findKey(final T key) {
      int i = 0;
      while (true) {
         if (i >= this.currentKeyCount) {
            break;
         } else if (this.keys[i].isNull()) {
            break;
         } else if (!this.greater(key, this.keys[i].key())) {
            break;
         }
         i++;
      }
      return i;
   }

   public BPlusTreeNode<T> insert(final T key, final Object value) {
      this.activate(ActivationPurpose.WRITE);
      // find the insertion place for key
      final int keyPosition = this.findKey(key);
      if (this.isLeaf) {
         // insert here
         this.insertKeyWithEntry(keyPosition, key, value);
      } else {
         // recursively insert in the appropriate child node
         this.children[keyPosition].insert(key, value);
      }
      // split if current node is too large
      if (this.isTooLarge()) {
         return this.split();
      }
      return null;
   }

   private void insertKey(final int keyPosition, final T key) {
      if (!this.keys[keyPosition].isNull()
            && this.keysEqual(this.keys[keyPosition].key(), key)) {
         // the key already exists.
         return;

      } else {
         // the key does not yet exist in this node.
         // make room
         for (int i = this.currentKeyCount - 1; i >= keyPosition; i--) {
            this.keys[i + 1] = this.keys[i];
            this.entries[i + 1] = this.entries[i];
         }

         // TODO should store a clone of the key instead of a reference to it!
         // Otherwise the caller of the insert method can change the key afterwards,
         // in which case it will probably be in the wrong place in the tree.

         // store the key and update the key count
         final BPlusTreeKey<T> newKey = new BPlusTreeKey<T>();
         newKey.setKey(key);
         this.keys[keyPosition] = newKey;
         this.currentKeyCount++;
         // create new entries
         this.entries[keyPosition] = new ArrayList<Object>();
      }
   }

   private void insertKeyWithEntry(final int keyPosition, final T key, final Object value) {
      // insert the key
      this.insertKey(keyPosition, key);

      // check if this instance of the value is already stored.
      for (final Object entry : this.entries[keyPosition]) {
         if (entry.equals(value)) {
            // found the same instance
            return;
         }
      }

      // add the value to the entries
      this.entries[keyPosition].add(value);
   }

   private void insertKeyWithEntries(final int keyPosition, final T key,
         final List<Object> entries) {
      for (final Object entry : entries) {
         this.insertKeyWithEntry(keyPosition, key, entry);
      }
   }

   private BPlusTreeNode<T> split() {
      if (this.isRoot()) {
         final BPlusTreeNode<T> newRoot = this.splitRoot();
         return newRoot;
      } else {
         this.splitNonRoot();
         return null;
      }
   }

   private BPlusTreeNode<T> splitNonRoot() {
      final int parentKeyPosition = this.parent.addKey(this.keys[this.medianIndex].key());
      final BPlusTreeNode<T> newNode = this.splitNode();
      newNode.parent = this.parent;
      this.parent.addChild(newNode, parentKeyPosition);

      // update previous/next references
      if (this.next != null) {
         this.link(newNode, this.next);
      }
      this.link(this, newNode);

      return newNode;
   }

   private BPlusTreeNode<T> splitRoot() {
      final BPlusTreeNode<T> newRoot = new BPlusTreeNode<T>(this.order, false);
      this.parent = newRoot;
      newRoot.addKey(this.keys[this.medianIndex].key());
      final BPlusTreeNode<T> newNode = this.splitNode();
      newNode.parent = newRoot;
      this.link(this, newNode);
      newRoot.addChild(this, -1);
      newRoot.addChild(newNode, 0);
      return newRoot;
   }

   private BPlusTreeNode<T> splitNode() {
      final int keyMoveCount = this.keys.length - this.medianIndex - 1;
      final BPlusTreeKey<T>[] newKeys = new BPlusTreeKey[keyMoveCount];

      // System.arraycopy(this.keys, this.medianIndex + 1, newKeys, 0, keyMoveCount);
      for (int i = 0; i < keyMoveCount; i++) {
         // newKeys[i] = this.keys[this.medianIndex + 1 + i];
         final BPlusTreeKey<T> currentKey = this.keys[this.medianIndex + 1 + i];
         final BPlusTreeKey<T> newKey = new BPlusTreeKey<T>();
         if (!currentKey.isNull()) {
            newKey.setKey(currentKey.key());
         }
         newKeys[i] = newKey;
      }

      List<Object>[] newEntries = new List[0];
      if (this.isLeaf) {
         newEntries = new List[keyMoveCount];
         System.arraycopy(this.entries, this.medianIndex + 1, newEntries, 0, keyMoveCount);
      }

      BPlusTreeNode<T> newNode = null;
      if (this.isLeaf) {
         newNode = new BPlusTreeNode<T>(this.parent, this.order, this.isLeaf, newKeys,
               newEntries);
      } else {
         final BPlusTreeNode<T>[] newChildren = new BPlusTreeNode[this.children.length
               - this.medianIndex - 1];
         System.arraycopy(this.children, this.medianIndex + 1, newChildren, 0,
               newChildren.length);
         newNode = new BPlusTreeNode<T>(this.parent, this.order, this.isLeaf, newKeys,
               newEntries, newChildren);
         for (final BPlusTreeNode<T> child : newChildren) {
            child.parent = newNode;
         }

         // remove the children that now belong to the new node from this one.
         for (int i = this.medianIndex + 1; i < this.children.length; i++) {
            this.children[i] = null;
         }
      }

      // remove the keys and entries that no longer belong to this node.
      // if isLeaf, the median element must stay here.
      // else, it must be removed here as it is now in the parent node.
      int start = this.medianIndex;
      if (this.isLeaf) {
         start++;
      }
      for (int i = start; i < this.keys.length; i++) {
         this.keys[i].removeKey();
         this.currentKeyCount--;
         if (this.isLeaf) {
            this.entries[i] = new ArrayList<Object>();
         }
      }

      return newNode;
   }

   private void link(final BPlusTreeNode<T> left, final BPlusTreeNode<T> right) {
      if (left != null) {
         left.next = right;
      }
      if (right != null) {
         right.previous = left;
      }
   }

   private void addChild(final BPlusTreeNode<T> child, final int keyPosition) {
      // child has to be inserted at keyPosition+1
      for (int i = this.currentKeyCount - 1; i > keyPosition; i--) {
         this.children[i + 1] = this.children[i];
      }
      child.parent = this;
      this.children[keyPosition + 1] = child;
   }

   private int addKey(final T key) {
      final int keyPosition = this.findKey(key);
      this.insertKey(keyPosition, key);
      return keyPosition;
   }

   private int addKeyWithEntries(final T key, final List<Object> entries) {
      final int keyPosition = this.findKey(key);
      this.insertKeyWithEntries(keyPosition, key, entries);
      return keyPosition;
   }

   public BPlusTreeNode<T> remove(final T key, final Object value) {
      this.activate(ActivationPurpose.WRITE);
      final BPlusTreeKey<T> replacement = new BPlusTreeKey<T>();
      return this.removeInternal(key, value, replacement, null, null, null, null);
   }

   private BPlusTreeNode<T> removeInternal(final T key, final Object value,
         final BPlusTreeKey<T> replacement, final BPlusTreeNode<T> leftSibling,
         final BPlusTreeNode<T> rightSibling, final BPlusTreeNode<T> leftAnchor,
         final BPlusTreeNode<T> rightAnchor) {

      this.activate(ActivationPurpose.WRITE);

      int keyPosition = this.findKey(key);
      BPlusTreeNode<T> nextLeft = null;
      BPlusTreeNode<T> nextRight = null;
      BPlusTreeNode<T> nextLeftAnchor = null;
      BPlusTreeNode<T> nextRightAnchor = null;
      BPlusTreeNode<T> nextNode = null;
      BPlusTreeNode<T> result = null;
      boolean containsKeyAsPivot = false;

      if (this.isLeaf) {
         // delete here
         final boolean keyRemoved = this.removeEntry(keyPosition, key, value);

         if (!keyRemoved) {
            // no need to do any further checks for re-balancing etc.
            return result;
         }

         // we might need a replacement for key as pivot on the path from the root to here.
         // choose as replacementKey
         // - the largest key left in this node if there still are keys
         // - the largest key of this node's left if there is a left sibling
         // - the smallest key of this node's right sibling if there is a right sibling
         T replacementKey = null;
         if (this.currentKeyCount > 0) {
            replacementKey = this.keys[this.currentKeyCount - 1].key();
         } else if (leftSibling != null) {
            replacementKey = leftSibling.keys[leftSibling.currentKeyCount - 1].key();
         } else if (rightSibling != null) {
            replacementKey = rightSibling.keys[0].key();
         } else {
            // There is no replacement.
            // Probably, the deleted key was the last entry in the tree.
         }
         replacement.setKey(replacementKey);

      } else {
         if (!this.keys[keyPosition].isNull()
               && this.keysEqual(this.keys[keyPosition].key(), key)) {
            // found the node on the on the path that contains key as a pivot
            // (there is at most one such node).
            // will carry along a reference to this node while recursively descending.
            // after deletion in the leaf, the key here can then directly be replaced
            // with the appropriate neighbour value in the leaf.
            containsKeyAsPivot = true;
         }

         // find siblings of this node and the anchors between this node and the siblings.
         if (keyPosition == 0) {
            if (leftSibling != null) {
               nextLeft = leftSibling.children[leftSibling.currentKeyCount];
            }
            nextLeftAnchor = leftAnchor;
         } else {
            nextLeft = this.children[keyPosition - 1];
            nextLeftAnchor = this;
         }

         if (keyPosition == this.currentKeyCount) {
            if (rightSibling != null) {
               nextRight = rightSibling.children[0];
            }
            nextRightAnchor = rightAnchor;
         } else {
            nextRight = this.children[keyPosition + 1];
            nextRightAnchor = this;
         }

         // recursively remove on the appropriate child node
         nextNode = this.children[keyPosition];
         result = nextNode.removeInternal(key, value, replacement, nextLeft, nextRight,
               nextLeftAnchor, nextRightAnchor);
      }

      if (result != null && !result.isRoot()) {
         // deletion caused a merge operation in a child node of this node.
         // the child that result points to is now empty and has to be removed from this
         // node.
         if (this.isLeaf) {
            // a leaf shouldn't have children, not even empty ones!
            throw new RuntimeException("Leaf has child");
         }

         final int emptyNodeIndex = this.findChild(result, keyPosition);

         if (this.children[emptyNodeIndex].currentKeyCount > 0) {
            throw new RuntimeException("Don't remove non-empty children");
         }

         this.removeChild(emptyNodeIndex);
         result = null;
      }

      // replace the deleted key with its replacement if necessary.
      if (containsKeyAsPivot) {
         // the keyPosition could have changed due to handleUnderflow
         keyPosition = this.findKey(key);
         if (!this.keys[keyPosition].isNull()
               && this.keysEqual(this.keys[keyPosition].key(), key)) {
            if (replacement.isNull()) {
               throw new RuntimeException("Replacement key has not been set.");
            }
            this.keys[keyPosition].setKey(replacement.key());
         }
      }

      if (this.isTooSmall()) {
         result = this.handleUnderflow(key, replacement, leftSibling, rightSibling,
               leftAnchor, rightAnchor);
      }

      return result;
   }

   private BPlusTreeNode<T> handleUnderflow(final T key, final BPlusTreeKey<T> replacement,
         final BPlusTreeNode<T> leftSibling, final BPlusTreeNode<T> rightSibling,
         final BPlusTreeNode<T> leftAnchor, final BPlusTreeNode<T> rightAnchor) {
      // in case of underflow, three possibilities exist to re-balance the tree:
      // 1) we are at the root -> collapse root.
      // 2) redistribute keys among siblings of the same parent node
      // 3) merge this node with a sibling, update the parent and possibly
      // propagate the underflow upwards in the tree.

      // 1) collapse root
      if (this.isRoot()) {
         // if we land here, we are at the root and the root has only one child left.
         // we therefore remove the current root and promote its only child to be
         // the new root.
         final BPlusTreeNode<T> newRoot = this.children[0];
         newRoot.makeRoot();
         return newRoot;
      }

      // 2) try redistribution among siblings
      int leftReserve = 0;
      int rightReserve = 0;
      if (leftSibling != null) {
         // this node has a sibling to the left
         leftReserve = leftSibling.sizeOverMinimum();
      }
      if (rightSibling != null) {
         // this node has a sibling to the right
         rightReserve = rightSibling.sizeOverMinimum();
      }

      if (leftReserve > rightReserve) {
         // redistribute with left sibling
         final int keyMoveCount = Math.max(1, leftReserve / 2);
         this.redistribute(leftSibling, this, leftAnchor, keyMoveCount, BPlusTreeNode.LEFT,
               key, replacement);
         return null;
      } else if (rightReserve > leftReserve) {
         // redistribute with right sibling
         final int keyMoveCount = Math.max(1, rightReserve / 2);
         this.redistribute(rightSibling, this, rightAnchor, keyMoveCount,
               BPlusTreeNode.RIGHT, key, replacement);
         return null;
      } else if (leftReserve > 0) {
         // both siblings have the same reserve, so just redistribute with the left one
         final int keyMoveCount = Math.max(1, leftReserve / 2);
         this.redistribute(leftSibling, this, leftAnchor, keyMoveCount, BPlusTreeNode.LEFT,
               key, replacement);
         return null;
      }

      // 3) merge with sibling
      // when arriving here: leftReserve == rightReserve == 0
      if (leftSibling != null && leftAnchor == this.parent) {
         // this node either has a left and a right sibling or just a left sibling.
         // merge with left sibling as both left and right will have the same size (s.a.) if
         // both are present.
         this.merge(leftSibling, leftAnchor, BPlusTreeNode.LEFT, key, replacement);
         return this;

      } else if (rightSibling != null && rightAnchor == this.parent) {
         // merge with right sibling
         this.merge(rightSibling, rightAnchor, BPlusTreeNode.RIGHT, key, replacement);
         return this;

      } else {
         // this node has no siblings. this is only cool, if we are at the root.
         // if isRoot, we should not be here anyway, since 1) should have applied.
         if (!this.isRoot()) {
            throw new RuntimeException("Node has no siblings and is not root!");
         }
      }
      return null;
   }

   private void merge(final BPlusTreeNode<T> sibling, final BPlusTreeNode<T> anchor,
         final boolean side, final T key, final BPlusTreeKey<T> replacement) {
      // merge equals redistributing all keys in this node to the sibling
      this
            .redistribute(this, sibling, anchor, this.currentKeyCount, !side, key,
                  replacement);

      if (this.currentKeyCount > 0) {
         throw new RuntimeException("This node should be empty now!");
      }

      // update the references to next/previous nodes
      BPlusTreeNode<T> left = null;
      BPlusTreeNode<T> right = null;
      if (!side == BPlusTreeNode.LEFT) {
         left = this.previous;
         right = sibling;
      } else {
         left = sibling;
         right = this.next;
      }
      this.link(left, right);
   }

   /**
    * Balances the key count between srcNode and destNode by moving half of reserveCount keys
    * from srcNode to destNode.
    *
    * @param srcNode
    *           Source node for redistribution. This is a sibling of destNode that contains
    *           more than enough keys.
    * @param destNode
    *           Destination node for redistribution. This node no longer contains enough
    *           keys.
    * @param anchor
    *           Parent node of both srcNode and destNode that contains the pivot element
    *           between those two nodes.
    * @param keyMoveCount
    *           Number of keys that should be moved from srcNode to destNode.
    * @param side
    *           Side, on which srcNode is as seen from destNode.
    * @param key
    *           The key who's deletion caused the underflow in destNode.
    * @param replacement
    *           Key containing the value to replace key with when found as pivot element.
    */
   private void redistribute(final BPlusTreeNode<T> srcNode,
         final BPlusTreeNode<T> destNode, final BPlusTreeNode<T> anchor,
         final int keyMoveCount, final boolean side, final T key,
         final BPlusTreeKey<T> replacement) {

      if (destNode.currentKeyCount + keyMoveCount >= destNode.keys.length) {
         throw new RuntimeException("Moving that many keys would (over)fill this node!");
      }
      if (srcNode.currentKeyCount < keyMoveCount) {
         throw new RuntimeException("Source node does not contain "
               + "the desired number of keys to be moved.");
      }

      int stillToMove = keyMoveCount;
      int srcIndex = 0;
      int destIndex = 0;
      int keyDiff = 0;
      int childDiff = 0;
      int pivotPosition;
      if (side == BPlusTreeNode.LEFT) {
         // keys from the a left srcNode are smaller than the ones stored in destNode.
         srcIndex = srcNode.currentKeyCount;
         destIndex = -1;
         keyDiff = -1;
         // find the position of the pivot key in anchor.keys that separates
         // srcNode and destNode
         if (srcNode.currentKeyCount > 0) {
            pivotPosition = anchor.findKey(srcNode.keys[0].key());
         } else {
            pivotPosition = anchor.findKey(destNode.keys[0].key()) - 1;
         }
      } else {
         destIndex = destNode.currentKeyCount;
         childDiff = 1;
         if (destNode.currentKeyCount > 0) {
            pivotPosition = anchor.findKey(destNode.keys[0].key());
         } else {
            pivotPosition = anchor.findKey(srcNode.keys[0].key()) - 1;
         }
      }

      // if we are not dealing with leaves, move the pivot value in anchor
      // that separates srcNode and destNode to destNode node.
      if (!destNode.isLeaf) {
         final BPlusTreeKey<T> pivotKey = new BPlusTreeKey<T>();
         if (anchor.keys[pivotPosition].key().compareTo(key) != 0) {
            pivotKey.setKey(anchor.keys[pivotPosition].key());
         } else {
            pivotKey.setKey(replacement.key());
         }
         if (!pivotKey.isNull()) {
            destNode.addKey(pivotKey.key());
         }
      }
      anchor.removeKey(pivotPosition);

      if (!destNode.isLeaf) {
         // transfer first child (that's the minimum to be transferred)
         destNode.addChild(srcNode.children[srcIndex], destIndex);
         srcNode.removeChild(srcIndex);
      }

      boolean rebalance = false;
      if (srcNode.currentKeyCount > stillToMove) {
         // we have a re-balancing operation.
         rebalance = true;
         // if we are at an internal node, the last key to be moved does not go
         // to destNode, but as new pivot to anchor. therefore we have to save
         // this key here from being moved to destNode.
         // (with leaves, the new anchor will be copied (not moved) to the anchor)
         if (!this.isLeaf) {
            stillToMove--;
         }
      } else {
         // srcNode.currentKeyCount == keyMoveCount
         // we have a merge operation. all keyMoveCount keys will be moved from
         // srcNode to destNode.
      }

      while (stillToMove > 0) {
         // transfer pairs of key & child from srcNode to destNode
         srcIndex += keyDiff;
         destIndex += childDiff;
         if (srcNode.isLeaf) {
            destNode.addKeyWithEntries(srcNode.keys[srcIndex].key(),
                  srcNode.entries[srcIndex]);
         } else {
            destNode.addKey(srcNode.keys[srcIndex].key());
         }
         srcNode.removeKey(srcIndex);
         if (!destNode.isLeaf) {
            destNode.addChild(srcNode.children[srcIndex], destIndex);
            srcNode.removeChild(srcIndex);
         }
         stillToMove--;
      }

      // if we are doing a re-balancing operation, we now have to move a new pivot to anchor
      if (rebalance) {
         if (srcIndex > 0) {
            srcIndex += keyDiff;
         }
         if (side == BPlusTreeNode.LEFT || !srcNode.isLeaf) {
            final BPlusTreeKey<T> replacementKey = srcNode.keys[srcIndex];
            if (!replacementKey.isNull()) {
               anchor.insertKey(pivotPosition, replacementKey.key());
               if (!srcNode.isLeaf) {
                  srcNode.removeKey(srcIndex);
               }
            }
         } else {
            final BPlusTreeKey<T> replacementKey = destNode.keys[destNode.currentKeyCount - 1];
            if (!replacementKey.isNull()) {
               anchor.insertKey(pivotPosition, replacementKey.key());
               if (!destNode.isLeaf) {
                  destNode.removeKey(destNode.currentKeyCount - 1);
               }
            }
         }
      }
   }

   private boolean removeEntry(final int keyPosition, final T key, final Object entry) {
      if (!this.keys[keyPosition].isNull()
            && this.keysEqual(this.keys[keyPosition].key(), key)) {
         // remove this entry from the entries for this key.
         // if it's the last one, also remove the key (and return true).
         final Iterator<Object> iterator = this.entries[keyPosition].iterator();
         while (iterator.hasNext()) {
            final Object e = iterator.next();
            if (e.equals(entry)) {
               iterator.remove();
               break;
            }
         }

         if (this.entries[keyPosition].size() == 0) {
            this.removeKey(keyPosition);
            return true;
         } else {
            return false;
         }

      } else {
         if (!this.keys[keyPosition].isNull()) {
            throw new RuntimeException("Found '" + this.keys[keyPosition].key()
                  + "' instead of the expected '" + key + "'");
         }
      }
      return false;
   }

   private void removeKey(final int keyPosition) {
      if (this.keys[keyPosition].isNull()) {
         // there is no value stored at this position
         return;
      }
      // move all keys and entries at positions > keyPosition one position down.
      for (int i = keyPosition; i < this.currentKeyCount; i++) {
         this.keys[i] = this.keys[i + 1];
         this.entries[i] = this.entries[i + 1];
      }
      this.keys[this.currentKeyCount] = new BPlusTreeKey<T>();
      this.entries[this.currentKeyCount] = new ArrayList<Object>();
      // decrement the currentKeyCount
      this.currentKeyCount--;
   }

   private void removeChild(final int index) {
      // move all children at positions > index one position down.
      for (int i = index; i <= this.currentKeyCount; i++) {
         this.children[i] = this.children[i + 1];
      }
      this.children[this.currentKeyCount + 1] = null;
   }

   /**
    * Compares the given keys lexicographically and returns whether key1 is greater than
    * key2, or not.
    *
    * @param key1
    *           The first key to be compared.
    * @param key2
    *           The second key to be compared.
    * @return <b>true</b> if key1 is strictly greater than key2<br>
    *         <b>false</b> else.
    */
   private boolean greater(final T key1, final T key2) {
      if (key1 == key2) {
         return false;
      }
      if (key1 == null) {
         return false;
      }
      if (key2 == null) {
         return true;
      }
      return key1.compareTo(key2) > 0;
   }

   private boolean keysEqual(final T key1, final T key2) {
      if (key1 == key2) {
         return true;
      }

      if (key1 == null || key2 == null) {
         // if both were null, the above would have applied.
         return false;
      }

      // both keys are != null so we can call compareTo.
      return key1.compareTo(key2) == 0;
   }

   private boolean isTooLarge() {
      return this.currentKeyCount == this.keys.length;
   }

   private boolean isTooSmall() {
      return this.sizeOverMinimum() < 0;
   }

   private int sizeOverMinimum() {
      if (this.isRoot()) {
         if (this.isLeaf) {
            return this.currentKeyCount;
         } else {
            // root must have at least 2 children
            if (this.children[1] != null) {
               return this.currentKeyCount - 1;
            } else {
               return -1;
            }
         }
      } else {
         return this.currentKeyCount - this.order + 1;
      }
   }

   private boolean isRoot() {
      return this.parent == null;
   }

   private void makeRoot() {
      this.parent = null;
   }

   /*****************************************************************************************
    * Querying
    ****************************************************************************************/

   /**
    * Adds all keys currently stored in the tree to keyList. This is accomplished by
    * traversing the tree's leaf level.
    *
    * @param resultList
    *           The list to which the keys should be added.
    */
   public void inOrder(final List<Object> resultList) {
      this.activate(ActivationPurpose.READ);
      if (!this.isLeaf) {
         this.children[0].inOrder(resultList);
      } else {
         this.addKeysToList(resultList);
         if (this.next != null) {
            this.next.inOrder(resultList);
         }
      }
   }

   private void addKeysToList(final List<Object> entryList) {
      this.addKeysToListFrom(0, entryList);
   }

   private void addKeysToListFrom(final int startPosition, final List<Object> entryList) {
      for (int i = startPosition; i < this.currentKeyCount; i++) {
         entryList.addAll(this.entries[i]);
      }
   }

   private void addKeysToListTo(final int endPosition, final List<Object> entryList) {
      // don't need BTreeNull values
      final int pos = Math.min(endPosition, this.currentKeyCount - 1);
      for (int i = pos; i >= 0; i--) {
         entryList.addAll(this.entries[i]);
      }
   }

   private void addKeysToListNotEqual(final List<Object> entryList, final T key) {
      for (int i = 0; i < this.currentKeyCount; i++) {
         if (!this.keysEqual(this.keys[i].key(), key)) {
            entryList.addAll(this.entries[i]);
         }
      }
   }

   public void greater(final T key, final List<Object> resultList, final boolean equal,
         boolean firstLeaf) {
      this.activate(ActivationPurpose.READ);
      final int keyPosition = this.findKey(key);
      if (!this.isLeaf) {
         this.children[keyPosition].greater(key, resultList, equal, firstLeaf);
      } else {
         int startPosition = keyPosition;
         if (firstLeaf) {
            if (!equal && !this.keys[keyPosition].isNull()
                  && this.keysEqual(this.keys[keyPosition].key(), key)) {
               startPosition++;
            }
            firstLeaf = false;
         }
         this.addKeysToListFrom(startPosition, resultList);
         if (this.next != null) {
            this.next.greater(key, resultList, equal, firstLeaf);
         }
      }
   }

   public void less(final T key, final List<Object> resultList, final boolean equal,
         boolean firstLeaf) {
      this.activate(ActivationPurpose.READ);
      final int keyPosition = this.findKey(key);
      if (!this.isLeaf) {
         this.children[keyPosition].less(key, resultList, equal, firstLeaf);
      } else {
         int endPosition = keyPosition;
         if (firstLeaf) {
            if (!equal && !this.keys[keyPosition].isNull()
                  && this.keysEqual(this.keys[keyPosition].key(), key)) {
               endPosition--;
            }
            if (endPosition >= 0 && !this.keys[endPosition].isNull()
                  && this.keys[endPosition].key().compareTo(key) > 0) {
               // the smallest entry in the tree is larger than key
               return;
            }
            firstLeaf = false;
         }
         this.addKeysToListTo(endPosition, resultList);
         if (this.previous != null) {
            this.previous.less(key, resultList, equal, firstLeaf);
         }
      }
   }

   public void notEqual(final T key, final List<Object> resultList) {
      this.activate(ActivationPurpose.READ);
      if (!this.isLeaf) {
         this.children[0].notEqual(key, resultList);
      } else {
         this.addKeysToListNotEqual(resultList, key);
         if (this.next != null) {
            this.next.notEqual(key, resultList);
         }
      }
   }

   /*****************************************************************************************
    * Integrity checking
    ****************************************************************************************/

   public void checkSubtreeIntegrity(final Collection<T> internalKeys)
         throws IntegrityException {
      this.activate(ActivationPurpose.READ);
      this.checkNodeIntegrity(internalKeys);
      if (!this.isLeaf) {
         for (int i = 0; i <= this.currentKeyCount; i++) {
            this.children[i].checkSubtreeIntegrity(internalKeys);
         }
      }
   }

   private void checkNodeIntegrity(final Collection<T> internalKeys)
         throws IntegrityException {
      this.checkKeys(internalKeys);
      this.checkSize();
      this.checkEntries();
      this.checkChildren();
      this.checkLeafLinks();
      if (!this.isLeaf) {
         this.checkKeyToChildKeyRelation();
      }
   }

   private void checkKeys(final Collection<T> internalKeys) throws IntegrityException {
      boolean nullValuesStarted = false;
      for (int i = 0; i < this.keys.length; i++) {
         if (this.keys[i].isNull()) {
            if (i < this.currentKeyCount - 1) {
               throw new IntegrityException("Missmatch between currentKeyCount ("
                     + this.currentKeyCount + ") and actual key count (" + i + ")");
            }
            nullValuesStarted = true;

         } else if (nullValuesStarted) {
            throw new IntegrityException("Found empty key between other keys");

         } else {
            if (i > 0) {
               // check if keys are sorted ascending
               if (this.keys[i - 1].key().compareTo(this.keys[i].key()) >= 0) {
                  throw new IntegrityException("Wrong order of keys: keys[" + (i - 1) + "]="
                        + this.keys[i - 1].key() + ", keys[" + i + "]=" + this.keys[i].key());
               }
            }
            if (!this.isLeaf) {
               if (internalKeys.contains(this.keys[i])) {
                  throw new IntegrityException("This key occurs twice as internal key: "
                        + this.keys[i]);
               }
               internalKeys.add(this.keys[i].key());
            }
         }
      }
   }

   private void checkEntries() throws IntegrityException {
      for (int i = 0; i < this.entries.length; i++) {
         if (!this.isLeaf && this.entries[i].size() > 0) {
            throw new IntegrityException("Non-leaf node contains entries");
         }

         if (i > this.currentKeyCount && this.entries[i].size() > 0) {
            throw new IntegrityException("Entries are not associated with a key");
         }

         final Collection<Object> es = new ArrayList<Object>();
         for (final Object entry : this.entries[i]) {
            if (es.contains(entry)) {
               throw new IntegrityException("The entry '" + entry
                     + "' occurs twice for the same key '" + this.keys[i] + "'");
            }
            es.add(entry);
         }
      }
   }

   private void checkChildren() throws IntegrityException {
      boolean nullValuesStarted = false;
      for (int i = 0; i < this.children.length; i++) {
         if (this.isLeaf) {
            if (this.children[i] != null) {
               throw new IntegrityException("Leaf has child");
            }

         } else if (this.children[i] == null) {
            if (i < this.currentKeyCount) {
               throw new IntegrityException("Not enough children (" + (i + 1)
                     + ") for currentKeyCount (" + this.currentKeyCount + ").");
            }
            nullValuesStarted = true;

         } else if (nullValuesStarted) {
            throw new IntegrityException("Found null between other children");

         } else {
            if (this.children[i].parent != this) {
               throw new IntegrityException("Childs parent reference isn't pointing to this");
            }
         }
      }
   }

   private void checkSize() throws IntegrityException {
      if (this.sizeOverMinimum() < 0) {
         throw new IntegrityException("Node is too small: Order='" + this.order
               + "' and currentKeyCount='" + this.currentKeyCount + "'");
      }
   }

   private void checkLeafLinks() throws IntegrityException {
      if (this.next == this || this.previous == this) {
         throw new IntegrityException("Node's previous or next reference points to itself");
      }
      if (this.next != null) {
         if (this.next.previous != this) {
            throw new IntegrityException(
                  "Nodes not linked correctly: this.next.previous != this");
         }
      }
      if (this.previous != null) {
         if (this.previous.next != this) {
            throw new IntegrityException(
                  "Nodes not linked correctly: this.previous.next != this");
         }
      }
   }

   private void checkKeyToChildKeyRelation() throws IntegrityException {
      T leftKey = null;
      T rightKey = null;
      for (int i = 0; i <= this.currentKeyCount; i++) {
         if (i > 0) {
            leftKey = this.keys[i - 1].key();
         }
         if (i < this.currentKeyCount) {
            rightKey = this.keys[i].key();
         } else {
            // i == currentKeyCount
            rightKey = null;
         }

         if (leftKey != null) {
            final BPlusTreeKey<T> childKey = this.children[i].keys[0];
            if (!childKey.isNull()) {
               // check that keys in child are greater than leftKey
               final T childKeyEntry = childKey.key();
               if (childKeyEntry.compareTo(leftKey) <= 0) {
                  throw new IntegrityException("Key in child (" + childKeyEntry
                        + ") is not greater than the left key in the parent (" + leftKey
                        + ")");
               }
            }
         }
         if (rightKey != null) {
            final BPlusTreeKey<T> childKey = this.children[i].keys[this.children[i].currentKeyCount - 1];
            if (!childKey.isNull()) {
               // check that keys in child are smaller or equal than rightKey
               final T childKeyEntry = childKey.key();
               if (childKeyEntry.compareTo(rightKey) > 0) {
                  throw new IntegrityException("Key in child (" + childKeyEntry
                        + ") is not smaller than or equal to the right key in the parent ("
                        + rightKey + ")");
               }
               if (this.children[i].isLeaf && childKeyEntry.compareTo(rightKey) != 0) {
                  throw new IntegrityException("Largest key in child-leaf (" + childKeyEntry
                        + ") is not equal to the right key in the parent (" + rightKey + ")");
               }
            }
         }
      }
   }

   /*****************************************************************************************
    * Activatable
    ****************************************************************************************/

   @Override
   public void activate(final ActivationPurpose ap) {
      if (this.activator != null) {
         this.activator.activate(ap);
      }
   }

   @Override
   public void bind(final Activator a) {
      if (this.activator == a) {
         return;
      }
      if (this.activator != null && a != null) {
         throw new IllegalStateException();
      }
      this.activator = a;
   }

}

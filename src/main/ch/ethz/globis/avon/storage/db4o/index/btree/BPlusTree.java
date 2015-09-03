/*
 * @(#)BTreeIndex.java   1.0   Feb 16, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: BPlusTree.java 4239 2009-07-10 08:32:40Z D\zimmerch $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.db4o.activation.ActivationPurpose;
import com.db4o.activation.Activator;
import com.db4o.ta.Activatable;

/**
 * Implementation of a B+ tree.
 * Access to the tree is managed by a ReentrantReadWriteLock. It allows several parallel read-operations.
 * If one write-operation is going on in the tree, it is locked for all other operations.
 *
 * @author Christoph Zimmerli &lt;zimmerch@ethz.ch&gt;
 * @version 1.0
 */
public class BPlusTree<T extends Comparable<T>> implements Activatable {

   private BPlusTreeNode<T> root;
   private int order;
   private final transient ReentrantReadWriteLock rwl;
   private final transient Lock readLock;
   private final transient Lock writeLock;
   private transient Activator activator;

   /**
    * Creates a new BPlusTree of given order.
    * The order determines, how many keys each node in the tree holds.
    * Given an order n &gt;= 2, each node (except the root) will hold m keys, where n &lt;= m &lt;= 2*n.
    *
    * @param order
    *          The tree's order.
    */
   public BPlusTree(final int order) {
      this();
      if (order < 2) {
         throw new IllegalArgumentException("Order must be at least '2'. Given: '" + order
               + "'");
      }
      this.order = order;
      this.root = new BPlusTreeNode<T>(order, true);
   }

   /**
    * Private constructor to be called by db4o when the tree is loaded from the database.
    */
   private BPlusTree() {
      this.rwl = new ReentrantReadWriteLock();
      this.readLock = this.rwl.readLock();
      this.writeLock = this.rwl.writeLock();
   }

   /**
    * Starts a self-test of the tree, where each node tests itself for consistency.
    *
    * @throws IntegrityException
    *          If the tree is not consistent.
    */
   public void checkIntegrity() throws IntegrityException {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final Collection<T> internalKeys = new ArrayList<T>();
         this.root.checkSubtreeIntegrity(internalKeys);
      } finally {
         this.readLock.unlock();
      }
   }

   /**
    * Returns whether the index contains the given key.
    *
    * @param key
    *           Key to be looked for in the index.
    * @return <b>true</b> if the index contains the key. <b>false</b> otherwise.
    */
   public boolean contains(final T key) {
      final Iterator<Object> result = this.get(key);
      return result.hasNext();
   }

   /**
    * Returns an iterator for the entries associated with the given key.
    *
    * @param key
    *           Key, whose corresponding entries should be returned.
    * @return The entries associated with the given key.
    */
   public Iterator<Object> get(final T key) {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         return this.root.search(key).iterator();
      } finally {
         this.readLock.unlock();
      }
   }

   private List<Object> inOrderList() {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final ArrayList<Object> list = new ArrayList<Object>();
         this.root.inOrder(list);
         return list;
      } finally {
         this.readLock.unlock();
      }
   }

   /**
    * Returns the number of entries (key/value pairs) currently stored in the index.
    *
    * @return Number of entries in the index.
    */
   public int size() {
      return this.inOrderList().size();
   }

   /**
    * Returns all entries in the index in order. The order is determined by the
    * implementation of the <tt>compareTo</tt> method inherited from <tt>Comparable</tt>.
    *
    * @return An Iterator containing all index entries in order.
    */
   public Iterator<Object> inOrder() {
      return this.inOrderList().iterator();
   }


   /**
    * Inserts the given key/value pair into the tree.
    *
    * @param key
    *          Key to be added to the tree.
    * @param value
    *          The value associated with the key.
    */
   public void insert(final Object key, final Object value) {
      this.activate(ActivationPurpose.WRITE);
      this.writeLock.lock();
      try {
         final BPlusTreeNode<T> newRoot = this.root.insert((T) key, value);
         if (newRoot != null) {
            this.root = newRoot;
         }
      } finally {
         this.writeLock.unlock();
      }
   }

   /**
    * Removes the given key/value pair from the tree.
    *
    * @param key
    *           Key to be removed from the tree.
    * @param value
    *          The value associated with the key.
    */
   public void remove(final Object key, final Object value) {
      this.activate(ActivationPurpose.WRITE);
      this.writeLock.lock();
      try {
         final BPlusTreeNode<T> newRoot = this.root.remove((T) key, value);
         if (newRoot != null) {
            this.root = newRoot;
         }
      } finally {
         this.writeLock.unlock();
      }
   }

   /**
    * Removes all entries from the index.
    */
   public void clear() {
      this.activate(ActivationPurpose.WRITE);
      this.writeLock.lock();
      try {
         this.root = new BPlusTreeNode<T>(this.order, true);
      } finally {
         this.writeLock.unlock();
      }
   }

   /**
    * Returns all entries in the index, that are strictly greater than the given key. The
    * order is defined by the implementation of the <tt>compareTo</tt> method inherited
    * from <tt>Comparable</tt>.
    *
    * @param key
    * @return An iterator containing all elements in the index that are strictly greater than
    *         the given key.
    */
   public Iterator<Object> greater(final T key) {
      return this.greater(key, false);
   }

   /**
    * Returns all entries in the index, that are greater than or equal to the given key. The
    * order is defined by the implementation of the <tt>compareTo</tt> method inherited
    * from <tt>Comparable</tt>.
    *
    * @param key
    * @return An iterator containing all elements in the index that are greater than or equal
    *         to the given key.
    */
   public Iterator<Object> greaterOrEqual(final T key) {
      return this.greater(key, true);
   }

   private Iterator<Object> greater(final T key, final boolean equal) {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final ArrayList<Object> list = new ArrayList<Object>();
         this.root.greater(key, list, equal, true);
         return list.iterator();
      } finally {
         this.readLock.unlock();
      }
   }

   /**
    * Returns all entries in the index, that are strictly less than the given key. The order
    * is defined by the implementation of the <tt>compareTo</tt> method inherited from
    * <tt>Comparable</tt>.
    *
    * @param key
    * @return An iterator containing all elements in the index that are strictly less than
    *         the given key.
    */
   public Iterator<Object> less(final T key) {
      return this.less(key, false);
   }

   /**
    * Returns all entries in the index, that are less than or equal to the given key. The
    * order is defined by the implementation of the <tt>compareTo</tt> method inherited
    * from <tt>Comparable</tt>.
    *
    * @param key
    * @return An iterator containing all elements in the index that are less than or equal to
    *         the given key.
    */
   public Iterator<Object> lessOrEqual(final T key) {
      return this.less(key, true);
   }

   private Iterator<Object> less(final T key, final boolean equal) {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final ArrayList<Object> list = new ArrayList<Object>();
         this.root.less(key, list, equal, true);
         return list.iterator();
      } finally {
         this.readLock.unlock();
      }
   }

   /**
    * Returns all entries in the index, that are equal to the given key. The order is defined
    * by the implementation of the <tt>compareTo</tt> method inherited from
    * <tt>Comparable</tt>.
    *
    * @param key
    * @return An iterator containing all elements in the index that are equal to the given
    *         key.
    */
   public Iterator<Object> equal(final T key) {
      return this.get(key);
   }

   /**
    * Returns all entries in the index, that are not equal to the given key. The order is
    * defined by the implementation of the <tt>compareTo</tt> method inherited from
    * <tt>Comparable</tt>.
    *
    * @param key
    * @return An iterator containing all elements in the index that are not equal to the
    *         given key.
    */
   public Iterator<Object> notEqual(final T key) {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final ArrayList<Object> list = new ArrayList<Object>();
         this.root.notEqual(key, list);
         return list.iterator();
      } finally {
         this.readLock.unlock();
      }
   }

   /**
    * Given a collection keys, this will return all elements of that given collection that
    * are part of the index. Example: Index contains the integers from 1 to 5.
    * index.in2([3,5,7]) returns [3,5].
    *
    * @param keys
    *           Collection of keys to be looked for in the index.
    * @return An iterator containing all elements of the given keys collection that are part
    *         of the index.
    */
   public Iterator<Object> in2(final Collection<T> keys) {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final ArrayList<Object> list = new ArrayList<Object>();
         this.root.inOrder(list);
         list.retainAll(keys);
         return list.iterator();
      } finally {
         this.readLock.unlock();
      }
   }

   /**
    * Returns all entries in the index that are like the given key. Likeness comparison is
    * implemented by comparing the string representation of the given key and the index
    * entries.
    *
    * @param key
    * @return An iterator containing all elements in the index that are like the given key.
    */
   public Iterator<Object> like(final T key) {
      this.activate(ActivationPurpose.WRITE);
      this.readLock.lock();
      try {
         final ArrayList<Object> list = new ArrayList<Object>();
         this.root.inOrder(list);
         final Iterator<Object> iterator = list.iterator();
         while (iterator.hasNext()) {
            if (!iterator.next().toString().toUpperCase().contains(
                  key.toString().toUpperCase())) {
               iterator.remove();
            }
         }
         return list.iterator();
      } finally {
         this.readLock.unlock();
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

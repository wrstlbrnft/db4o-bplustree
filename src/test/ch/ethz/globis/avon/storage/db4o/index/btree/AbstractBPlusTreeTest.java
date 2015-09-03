/*
 * @(#)AbstractBTreeTest.java   1.0   Apr 3, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: AbstractBPlusTreeTest.java 4215 2009-07-07 18:46:31Z D\michagro $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import java.util.Iterator;

import ch.ethz.globis.avon.storage.db4o.index.btree.BPlusTree;
import ch.ethz.globis.avon.storage.db4o.index.btree.IntegrityException;


abstract class AbstractBPlusTreeTest {
   
   private BPlusTree tree;
   
   protected BPlusTree tree() {
      return this.tree;
   }
   
   protected void setTree(final BPlusTree tree) {
      this.tree = tree;
   }
   
   protected void insertKeys() throws IntegrityException {
      this.tree.insert(8, 8);
      this.tree.checkIntegrity();
      this.tree.insert(1, 1);
      this.tree.checkIntegrity();
      this.tree.insert(6, 6);
      this.tree.checkIntegrity();
      this.tree.insert(4, 4);
      this.tree.checkIntegrity();
      this.tree.insert(2, 2);
      this.tree.checkIntegrity();
      this.tree.insert(3, 3);
      this.tree.checkIntegrity();
      this.tree.insert(5, 5);
      this.tree.checkIntegrity();
      this.tree.insert(7, 7);
      this.tree.checkIntegrity();
      this.tree.insert(10, 10);
      this.tree.checkIntegrity();
      this.tree.insert(9, 9);
      this.tree.checkIntegrity();
   }

   protected void insertKeys2() throws IntegrityException {
      this.tree.insert(5, 5);
      this.tree.checkIntegrity();
      this.tree.insert(6, 6);
      this.tree.checkIntegrity();
      this.tree.insert(7, 7);
      this.tree.checkIntegrity();
      this.tree.insert(1, 1);
      this.tree.checkIntegrity();
      this.tree.insert(2, 2);
      this.tree.checkIntegrity();
      this.tree.insert(3, 3);
      this.tree.checkIntegrity();
      this.tree.insert(4, 4);
      this.tree.checkIntegrity();
      this.tree.insert(8, 8);
      this.tree.checkIntegrity();
      this.tree.insert(9, 9);
      this.tree.checkIntegrity();
      this.tree.insert(10, 10);
      this.tree.checkIntegrity();
      this.tree.insert(11, 11);
      this.tree.checkIntegrity();
   }

   protected void insertStrings() {
      this.tree.insert("a", "a");
      this.tree.insert("b", "b");
      this.tree.insert("f", "f");
      this.tree.insert("foif", "foif");
      this.tree.insert("aaaaa", "aaaaa");
      this.tree.insert("aaa", "aaa");
      this.tree.insert("aa", "aa");
      this.tree.insert("A", "A");
   }
   
   protected int printIterator(final Iterator iterator) {
      int itemCount = 0;
      while (iterator.hasNext()) {
         System.out.print(iterator.next());
         if (iterator.hasNext()) {
            System.out.print(", ");
         }
         itemCount++;
      }
      System.out.println();
      return itemCount;
   }
}

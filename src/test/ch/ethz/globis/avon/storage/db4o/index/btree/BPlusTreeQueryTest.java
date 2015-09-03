/*
 * @(#)BTreeQueryTest.java   1.0   Apr 3, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: BPlusTreeQueryTest.java 4215 2009-07-07 18:46:31Z D\michagro $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import ch.ethz.globis.avon.storage.db4o.index.btree.BPlusTree;
import ch.ethz.globis.avon.storage.db4o.index.btree.IntegrityException;


public class BPlusTreeQueryTest extends AbstractBPlusTreeTest {
   
   @Test
   public void greater() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();   // inserts the integers 1 to 10
      
      System.out.print("items > 5: ");
      Iterator iterator = this.tree().greater(5);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(5, itemCount);
      
      System.out.print("items > -3: ");
      iterator = this.tree().greater(-3);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(10, itemCount);
      
      System.out.print("items > 11: ");
      iterator = this.tree().greater(11);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(0, itemCount);
   }
   
   @Test
   public void greaterOrEqual() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();   // inserts the integers 1 to 10
      
      System.out.print("items >= 5: ");
      final Iterator iterator = this.tree().greaterOrEqual(5);
      final int itemCount = this.printIterator(iterator);
      Assert.assertEquals(6, itemCount);
   }
   
   @Test
   public void less() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
      
      System.out.print("items < 5: ");
      Iterator iterator = this.tree().less(5);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(4, itemCount);
      
      System.out.print("items < 23: ");
      iterator = this.tree().less(23);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(10, itemCount);
      
      System.out.print("items < -5: ");
      iterator = this.tree().less(-5);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(0, itemCount);
   }

   @Test
   public void lessOrEqual() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
      
      System.out.print("items <= 7: ");
      Iterator iterator = this.tree().lessOrEqual(7);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(7, itemCount);
      
      System.out.print("items <= 1: ");
      iterator = this.tree().lessOrEqual(1);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(1, itemCount);
      
      System.out.print("items <= 0: ");
      iterator = this.tree().lessOrEqual(0);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(0, itemCount);
   }
   
   @Test
   public void equal() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(3));
      this.insertKeys();
      
      System.out.print("items = 3: ");
      Iterator iterator = this.tree().equal(3);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(1, itemCount);
         
      System.out.print("items = 42: ");
      iterator = this.tree().equal(42);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(0, itemCount);
   }
   
   @Test
   public void notEqual() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
      
      System.out.print("items != 3: ");
      Iterator iterator = this.tree().notEqual(3);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(9, itemCount);
         
      System.out.print("items != 42: ");
      iterator = this.tree().notEqual(42);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(10, itemCount);
   }
   
   @Test
   public void in() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
      final Collection keys = new HashSet();
      keys.add(1);
      keys.add(3);
      keys.add(5);
      
      System.out.print("items IN2 '1,3,5': ");
      Iterator iterator = this.tree().in2(keys);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(3, itemCount);
      
      keys.clear();
      keys.add(7);
      keys.add(9);
      keys.add(11);
      keys.add(13);
      System.out.print("items IN2 '7,9,11,13': ");
      iterator = this.tree().in2(keys);
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(2, itemCount);
   }
   
   @Test
   public void like() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
      
      System.out.print("items like '1': ");
      Iterator iterator = this.tree().like(1);
      int itemCount = this.printIterator(iterator);
      Assert.assertEquals(2, itemCount);
      
      this.setTree(new BPlusTree<String>(2));
      this.insertStrings();
      
      System.out.print("items like 'a': ");
      iterator = this.tree().like('a');
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(5, itemCount);
      
      System.out.print("items like 'F': ");
      iterator = this.tree().like("F");
      itemCount = this.printIterator(iterator);
      Assert.assertEquals(2, itemCount);
   }
   
}

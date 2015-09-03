/*
 * @(#)BTreeTest.java   1.0   Feb 16, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: BPlusTreeTest.java 4215 2009-07-07 18:46:31Z D\michagro $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import java.io.File;
import java.util.Iterator;
import java.util.Random;

import com.db4o.Db4oEmbedded;
import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.query.Query;
import com.db4o.ta.TransparentPersistenceSupport;

import org.junit.Assert;
import org.junit.Test;

import ch.ethz.globis.avon.storage.db4o.index.btree.BPlusTree;
import ch.ethz.globis.avon.storage.db4o.index.btree.BPlusTreeKey;
import ch.ethz.globis.avon.storage.db4o.index.btree.IntegrityException;

public class BPlusTreeTest extends AbstractBPlusTreeTest {

   private static final boolean RAMDOMISED_TEST_DEBUG = false;
   private static final int RAMDOMISED_TEST_COUNT = 20;
   private static final int RANDOMISED_TEST_MAX_TREE_ORDER = 512;
   private static final int RANDOMISED_TEST_MAX_KEY = 1000;
   private static final int RANDOMISED_TEST_MIN_ITERATIONS = 100;
   private static final int RANDOMISED_TEST_MAX_ITERATIONS = 1000;


   @Test(expected = IllegalArgumentException.class)
   public void order() {
      new BPlusTree<Integer>(1);
   }

   @Test
   public void empty() {
      this.setTree(new BPlusTree<Integer>(2));
      final Integer key = new Integer(0);
      Assert.assertFalse(this.tree().contains(key));
   }

   @Test
   public void insert() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
   }

   @Test
   public void insert2() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys2();
   }


   @Test
   public void contains() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys();
      Assert.assertTrue(this.tree().contains(8));
      Assert.assertTrue(this.tree().contains(1));
      Assert.assertTrue(this.tree().contains(6));
      Assert.assertTrue(this.tree().contains(4));
      Assert.assertTrue(this.tree().contains(2));
      Assert.assertTrue(this.tree().contains(3));
      Assert.assertTrue(this.tree().contains(5));
      Assert.assertTrue(this.tree().contains(7));
      Assert.assertTrue(this.tree().contains(10));
      Assert.assertTrue(this.tree().contains(9));

      Assert.assertFalse(this.tree().contains(0));
      Assert.assertFalse(this.tree().contains(11));
   }

   @Test
   public void contains2() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys2();
      Assert.assertTrue(this.tree().contains(5));
      Assert.assertTrue(this.tree().contains(6));
      Assert.assertTrue(this.tree().contains(7));
      Assert.assertTrue(this.tree().contains(1));
      Assert.assertTrue(this.tree().contains(2));
      Assert.assertTrue(this.tree().contains(3));
      Assert.assertTrue(this.tree().contains(4));
      Assert.assertTrue(this.tree().contains(8));
      Assert.assertTrue(this.tree().contains(9));
      Assert.assertTrue(this.tree().contains(10));
      Assert.assertTrue(this.tree().contains(11));

      Assert.assertFalse(this.tree().contains(-1));
      Assert.assertFalse(this.tree().contains(12));
   }

   @Test
   public void remove() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertKeys2();
      this.tree().checkIntegrity();

      this.tree().remove(5, 5);
      Assert.assertFalse(this.tree().contains(5));
      this.tree().checkIntegrity();

      this.tree().remove(1, 1);
      Assert.assertFalse(this.tree().contains(1));
      this.tree().checkIntegrity();

      this.tree().remove(2, 2);
      Assert.assertFalse(this.tree().contains(2));
      this.tree().checkIntegrity();

      this.tree().remove(3, 3);
      Assert.assertFalse(this.tree().contains(3));
      this.tree().checkIntegrity();

      this.tree().remove(9, 9);
      Assert.assertFalse(this.tree().contains(9));
      this.tree().checkIntegrity();

      this.tree().remove(7, 7);
      Assert.assertFalse(this.tree().contains(7));
      this.tree().checkIntegrity();

      this.tree().remove(6, 6);
      Assert.assertFalse(this.tree().contains(6));
      this.tree().checkIntegrity();

      Assert.assertTrue(this.tree().contains(4));
      Assert.assertTrue(this.tree().contains(8));
      Assert.assertTrue(this.tree().contains(10));
      Assert.assertTrue(this.tree().contains(11));
   }


   @Test
   public void clear() throws IntegrityException {
      this.insert();
      this.tree().clear();
      this.tree().checkIntegrity();

      Assert.assertFalse(this.tree().contains(8));
      Assert.assertFalse(this.tree().contains(1));
      Assert.assertFalse(this.tree().contains(6));
      Assert.assertFalse(this.tree().contains(4));
      Assert.assertFalse(this.tree().contains(2));
      Assert.assertFalse(this.tree().contains(3));
      Assert.assertFalse(this.tree().contains(5));
      Assert.assertFalse(this.tree().contains(7));
      Assert.assertFalse(this.tree().contains(10));
      Assert.assertFalse(this.tree().contains(9));
   }


   @Test
   public void randomisedTests() throws IntegrityException {
      // test small values for treeOrder thoroughly, as problems occur here more often
      for (int i = 2; i < 10; i++) {
         this.randomInsertAndRemove(i, 20, 300);
         this.randomInsertAndRemove(i, 100, 500);
         //this.randomInsertAndRemove(i, 750, 10000);
      }

      // go for some more, now all random
      final Random random = new Random();
      for (int i = 0; i < BPlusTreeTest.RAMDOMISED_TEST_COUNT; i++) {
         int treeOrder = random.nextInt(BPlusTreeTest.RANDOMISED_TEST_MAX_TREE_ORDER);
         if (treeOrder < 2) {
            treeOrder += 2;
         }
         int maxKey = 0;
         while (maxKey == 0) {
            maxKey = random.nextInt(BPlusTreeTest.RANDOMISED_TEST_MAX_KEY);
         }
         int iterations = random.nextInt(BPlusTreeTest.RANDOMISED_TEST_MAX_ITERATIONS);
         if (iterations < BPlusTreeTest.RANDOMISED_TEST_MIN_ITERATIONS) {
            iterations += BPlusTreeTest.RANDOMISED_TEST_MIN_ITERATIONS;
         }
         this.randomInsertAndRemove(treeOrder, maxKey, iterations);
      }
   }

   private void randomInsertAndRemove(final int treeOrder, final int maxKey,
         final int iterations) throws IntegrityException {
      System.out.println("Running test with treeOrder='" + treeOrder + "', maxKey='"
            + maxKey + "' and iterations='" + iterations + "'");
      this.setTree(new BPlusTree<Integer>(treeOrder));
      final Random random = new Random();
      for (int i = 1; i < iterations; i++) {
         final Comparable<Integer> key = random.nextInt(maxKey);
         if (this.tree().contains(key)) {
            if (BPlusTreeTest.RAMDOMISED_TEST_DEBUG) {
               System.out.println("this.tree.remove(" + key + ", " + key + ");");
            }
            this.tree().remove(key, key);
         } else {
            if (BPlusTreeTest.RAMDOMISED_TEST_DEBUG) {
               System.out.println("this.tree.insert(" + key + ", " + key + ");");
            }
            this.tree().insert(key, key);
         }
         this.tree().checkIntegrity();
      }
      if (BPlusTreeTest.RAMDOMISED_TEST_DEBUG) {
         System.out.println("-------------------------------");
      }
   }

   @Test
   public void inOrderIterator() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      System.out.println("inserting keys from '1' to '11'");
      this.insertKeys2();
      System.out.println("printing contents of inOrder iterator:");
      final Iterator<Comparable> iterator = this.tree().inOrder();
      this.printIterator(iterator);
   }




   @Test
   public void activation() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      final String dbFileName = "tree_test.db4o";
      ObjectContainer db = Db4oEmbedded.openFile(this.configuration(), dbFileName);
      try {
         db.store(this.tree());

         this.tree().insert(1, 1);
         this.tree().insert(2, 2);
         db.commit();
         db.close();
         this.setTree(null);

         db = Db4oEmbedded.openFile(this.configuration(), dbFileName);
         final Query query = db.query();
         query.constrain(BPlusTree.class);
         final ObjectSet<Object> result = query.execute();
         Assert.assertEquals(1, result.size());
         this.setTree((BPlusTree<Integer>) result.next());

         this.tree().checkIntegrity();
         Assert.assertTrue(this.tree().contains(1));
         Assert.assertTrue(this.tree().contains(2));
      } finally {
         db.close();
         new File(dbFileName).delete();
      }
   }

   private EmbeddedConfiguration configuration() {
      final EmbeddedConfiguration configuration = Db4oEmbedded.newConfiguration();
      configuration.common().add(new TransparentPersistenceSupport());
      configuration.common().objectClass(BPlusTree.class).callConstructor(true);
      return configuration;
   }

   @Test
   public void stringTree() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(3));
      this.insertStrings();
      this.tree().checkIntegrity();
      final Iterator<Comparable> iterator = this.tree().inOrder();
      this.printIterator(iterator);
   }



   @Test
   public void mixedTree() {
      this.setTree(new BPlusTree<Integer>(2));
      this.insertStrings();
      try {
         this.tree().insert(new Integer(4), new Integer(4));
         Assert.fail("expected exception was not thrown");
      } catch (final ClassCastException e) {
         // expected exception from using compareTo on a String and an Integer
      }
   }

   @Test
   public void booleanTree() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.tree().insert(new Boolean(true), new Boolean(true));
      this.tree().insert(new Boolean(false), new Boolean(false));
      this.tree().checkIntegrity();
      this.printIterator(this.tree().inOrder());
   }

   @Test
   public void multipleEntries() throws IntegrityException {
      this.setTree(new BPlusTree<Integer>(2));
      this.tree().insert(1, 1);
      this.tree().insert(1, 11);
      this.tree().checkIntegrity();
      this.tree().insert(1, 111);
      this.tree().checkIntegrity();
      this.tree().insert(2, 2);
      this.tree().checkIntegrity();
      this.tree().insert(2, 22);
      this.tree().checkIntegrity();

      int count = this.printIterator(this.tree().inOrder());
      Assert.assertEquals(5, count);
      count = this.printIterator(this.tree().greater(1));
      Assert.assertEquals(2, count);
      count = this.printIterator(this.tree().equal(1));
      Assert.assertEquals(3, count);
   }

   @Test
   public void nullKeys() {
      this.setTree(new BPlusTree<String>(2));
      this.tree().insert(null, 1);
      Assert.assertTrue(this.tree().contains(null));
      this.tree().remove(null, 1);
   }

   @Test
   public void emptyKeyException() {
      final BPlusTreeKey<Integer> key = new BPlusTreeKey<Integer>();
      try {
         key.key();
         Assert.fail();
      } catch (final Exception e) {
         // test passed
      }
   }
}

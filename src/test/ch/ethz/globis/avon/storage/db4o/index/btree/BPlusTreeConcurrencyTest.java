/*
 * @(#)BPlusTreeConcurrencyTest.java   1.0   Jul 8, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id$
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import java.util.Random;

import org.junit.Test;

import ch.ethz.globis.avon.storage.db4o.index.btree.BPlusTree;


public class BPlusTreeConcurrencyTest {

   private static final int THREAD_COUNT = 5;

   private BPlusTree<Integer> tree;
   private Thread[] threads;

   public static void main(final String[] args) {
      new BPlusTreeConcurrencyTest().run();
   }

   @Test
   public void test() {
      this.run();
   }

   private void run() {
      this.tree = new BPlusTree<Integer>(3);
      this.threads = new Thread[BPlusTreeConcurrencyTest.THREAD_COUNT];
      for (int i = 0; i < BPlusTreeConcurrencyTest.THREAD_COUNT; i++) {
         this.threads[i] = new Thread(new TreeManipulator(i + 1, this.tree));
      }
      for (int i = 0; i < BPlusTreeConcurrencyTest.THREAD_COUNT; i++) {
         this.threads[i].start();
      }
   }

}

class TreeManipulator implements Runnable {

   private static final int OPERATION_COUNT = 20;

   private static final String INDENT = "" + '\t' + '\t' + '\t';

   private final BPlusTree<Integer> tree;
   private final int threadNumber;
   private final Random random;
   private final String indent;


   public TreeManipulator(final int threadNumber, final BPlusTree<Integer> tree) {
      this.threadNumber = threadNumber;
      this.tree = tree;
      this.random = new Random();
      final StringBuffer buffer = new StringBuffer();
      for (int i = 0; i < this.threadNumber; i++) {
         buffer.append(TreeManipulator.INDENT);
      }
      this.indent = buffer.toString();
   }

   @Override
   public void run() {
      System.out.println(this.indent + "This is thread " + this.threadNumber);
      for (int i = 0; i < TreeManipulator.OPERATION_COUNT; i++) {
         this.performOperation();
         try {
            Thread.sleep(50);
         } catch (final InterruptedException e) {
            e.printStackTrace();
         }
      }
   }

   private void performOperation() {
      final Integer key = Integer.valueOf(this.random.nextInt());
      if (this.random.nextBoolean()) {
         this.performWrite(key);
      } else {
         this.performRead(key);
      }
   }

   private void performWrite(final Integer key) {
      final Object value = new Object();
      System.out.println(this.indent + "Starting write");
      this.tree.insert(key, value);
      System.out.println(this.indent + "Write complete");
   }

   private void performRead(final Integer key) {
      System.out.println(this.indent + "Starting read");
      this.tree.contains(key);
      System.out.println(this.indent + "Read complete");
   }

}

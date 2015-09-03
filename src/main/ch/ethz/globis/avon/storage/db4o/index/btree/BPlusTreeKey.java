/*
 * @(#)BPlusTreeKey.java   1.0   Apr 23, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: BPlusTreeKey.java 4235 2009-07-09 14:15:43Z D\zimmerch $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

import com.db4o.activation.ActivationPurpose;
import com.db4o.activation.Activator;
import com.db4o.ta.Activatable;

/**
 * Representation of a key stored in a B+ Tree.
 * Distinguishes between storing <code>null</code> as key and the key not being set.
 *
 * @author Christoph Zimmerli &lt;zimmerch@ethz.ch&gt;
 * @version 1.0
 */
public class BPlusTreeKey<T extends Comparable<T>> implements Activatable {

   private boolean isNull;
   private T key;

   private transient Activator activator;

   public BPlusTreeKey() {
      this.isNull = true;
      this.key = null;
   }

   public T key() {
      this.activate(ActivationPurpose.READ);
      if (this.isNull) {
         throw new RuntimeException("This key is empty!");
      }
      return this.key;
   }

   public void setKey(final T key) {
      this.activate(ActivationPurpose.WRITE);
      this.key = key;
      this.isNull(false);
   }

   public void removeKey() {
      this.activate(ActivationPurpose.WRITE);
      this.key = null;
      this.isNull(true);
   }

   public boolean isNull() {
      this.activate(ActivationPurpose.READ);
      return this.isNull;
   }

   private void isNull(final boolean value) {
      this.activate(ActivationPurpose.WRITE);
      this.isNull = value;
   }

   @Override
   public String toString() {
      this.activate(ActivationPurpose.READ);
      if (this.isNull) {
         return "NULL";
      }
      return this.key.toString();
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

/*
 * @(#)IntegrityException.java   1.0   Mar 4, 2009
 *
 * Copyright 2000-2009 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: IntegrityException.java 3980 2009-04-20 15:43:10Z D\michagro $
 */
package ch.ethz.globis.avon.storage.db4o.index.btree;

public class IntegrityException extends Exception {

   private static final long serialVersionUID = 1L;

   public IntegrityException(final String message) {
      super(message);
   }
   
}

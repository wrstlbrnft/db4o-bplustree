# db4o-bplustree

## About the Project

This project is an implementation of a B+ tree in Java. For general information about B+ trees and in particular the deletion algorithm used in this implementation, see [this paper](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.128.4051).
Among this implementation's key features are:

* Implements db4o's `Activatable` interface and hence supports TransparentActivation and -Persistence
* Supports generics
* Supports several entries for the same key

## Comparable Keys

Keys are compared using the compareTo method defined in Javas's `Comparable` interface. 
This means that keys should implement `Comparable` and all keys stored in the same tree should have an implementation of `compareTo` that supports the other key's types.

## Concurrent Access

The tree uses a read-write lock to manage concurrent access. If one thread is reading the tree, others are allowed to read as well. If a thread is writing in the tree, the whole tree is locked for all other threads. This is due to the fact that a write operation (insert or delete) can possibly propagate up from the leaf level to the root and even replace the root.

## Usage and Unit Tests

In order to create a new B+ tree, you need to specify the tree's order and the generic type (if you like). To understand the order parameter, please consult the constructor's Javadoc. In code, creating a new tree of order 3 for keys of type Integer looks like this:

```java
BPlusTree tree = new BPlusTree<Integer>(3);
```

When storing the tree in db4o, configure db4o to call constructors on BPlusTree class to assign a new read-write lock when retrieving a tree from the database:

```java
EmbeddedConfiguration configuration = Db4oEmbedded.newConfiguration();
configuration.common().objectClass(BPlusTree.class).callConstructor(true);
```

The read-write lock mentioned above is a transient field of `BPlusTree` (lock state will not be saved in the db). The constructor call is necessary to create a new lock instance when retrieving the tree from a database.

The tree has been tested with db4o versions 7.8.82, 7.10.96 and 8.0.184.

The test cases provided with with the source code are written for JUnit 4. They also serve as examples of how to, and how not to use the B+ tree.

## Code Organisation

All classes are part of the package `ch.ethz.globis.avon.storage.db4o.index.btree`.
The source code is divided into the implementation itself in the directory `src/main/` and the test cases in `src/test/`.

## Background

This tree was implemented as part of [OMS Avon](http://maven.globis.ethz.ch/projects/avon/). This is also visible from the package path shown above.
The tree was also made available on the db4o Website (see cached version on [archive.org](https://web.archive.org/web/20090919010248/http://developer.db4o.com/ProjectSpaces/view.aspx/BPlusTree)).

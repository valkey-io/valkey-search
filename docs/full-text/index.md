# Text Index

The _TextIndex_ object is logically a sequence of 4-tuples: (_Word_, _Key_, _Field_, _Position_), as will be seen the search operators can be streamlined when the tuple can be iterated in that order, henceforth referred to as lexical order.
Lexical ordering allows operations like intersection and union that operate on multiple
iteration sequences to perform merge-like operations in linear time.

In addition to the standard CRUD operations _TextIndex_ provides _WordIterator_ that efficiently iterates over sequences of tuples where the _Word_ element shares a common prefix or
optionally a common suffix, again in lexical order. _WordIterator_ optimizes other operations, e.g.,
it's efficient to move from one _Key_ to a next _Key_ without iterating over the intervening _Field_ and/or _Position_ entries -- typically in O(1) or worst case O(log #keys) time.
From this capability is constructed the various search operators: word search, phrase search, and fuzzy search.

The _TextIndex_ object is implemented as a two-level hierarchy of objects. At the top level is a _RadixTree_ which maps a _Word_ into a _Postings_ object which is a container of (_Key_, _Field_, _Position_) triples.
The use of the top-level _RadixTree_ allows efficient implementation of operations on subsets of the index that consist of _Words_ that have a common prefix or suffix.

Both the _Postings_ and _RadixTree_ implementations must adapt efficiently across a wide range in the number of items they contain.
It's expected that both objects will have multiple internal representations to balance time/space efficiency at different scales.
Likely the initial implementation will have two representations, i.e.,
a space-efficient implementation with O(N) insert/delete/iterate times and a time-efficient implementation with O(1) or O(Log N) insert/delete/iterate times.

Like all of the Valkey search operators, the text search operators: word, phrase and fuzzy search must support both the pre-filtering and post-filtering modes when combined with vector search.
At the conceptual level, the only real difference between the pre- and post- filtering modes of the search operators is that for the post-filtering mode the search is performed across all _TextIndex_ entries with a particular _Field_. Whereas for the pre-filtering mode the search is performed for _TextIndex_ entries with a particular _Key_.

While there are many time/space tradeoffs possible for the pre-filtering case, it is proposed to handle the pre-filtering case with the same code as the post-filtering case only operating over a _TextIndex_ that has been constrained to a single _Key_.
In other words, for each user-declared Schema there will be one _TextIndex_ constructed across all of the _Key_, _Field_ and _Position_ entries. This _TextIndex_ object will support all non-vector and post-filtered vector query operations. In addition, each Schema will have a secondary hashmap that provides one _TextIndex_ object for each _Key_ to support pre-filtering vector queries.

As it turns out, this secondary per-key hashmap is also useful to support key deletion as it contains exactly the words contained by the fields of the key and nothing else. This use-case drives the need for the _RadixTree_ and _Postings_ objects to have representations optimized for a very low numbers of entries.

## Defrag

The _Postings_ objects contained within the schema-wide _TextIndex_ object will contain the majority of the consumed memory. Implementing defrag is done by using the _WordIterator_ to visit each _Postings_ object node and defrag it.

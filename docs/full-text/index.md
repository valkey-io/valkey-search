# Text Index
The per-_index_ text index is logically a sequence of quadruple tuples: (_Word_, _Key_, _Field_, _Position_), as will be seen the search operators can be streamlined when the tuple elements can be sequenced in a consistent order, henceforth referred to as lexical order. 
The lexical ordering allows operations like intersection and union that operate on multiple
 iteration sequences to perform merge-like operations in linear time. 

In addition to the standard CRUD operations the index efficiently supports iterating over sequences where the _word_ shares a common prefix and 
optionally a common suffix, again in lexical order. Other iteration sequences are optimized, for example within a word, 
it's efficient to move from one _key_ to a next _key_ without iterating over the intervening _field_ and/or _position_ entries -- typically in O(1) or worst case O(log #keys) time. 
From this capability is constructed the various search operators: word search, phrase search, and fuzzy search.

This mapping is implemented as a two-level hierarchy of objects. At the top level is a radix tree which map words into a Postings object which is a container of (_Key_, _field_ _Position_) triples. 
The use of the top-level radix tree allows efficient implementation of operations on subsets of the index that consist of words that have a common prefix and/or suffix.

Both the Postings and Radix tree implementations must adapt efficiently across a wide range in the number of items they contain. 
It's expected that both objects will have multiple internal representations to balance time/space efficiency at different scales.
Likely the initial implementation will have two representstions, i.e., 
a space-efficient implementation with O(N) insert/delete/iterate times and a time-efficient implementation with O(1) or O(Log N) insert/delete/iterate times.

Like all of the Valkey search operators, the text search operators: word, phrase and fuzzy search must support both the pre-filtering and post-filtering modes when combined with vector search.
At the conceptual level, the only real difference between the pre- and post- filtering modes of the search operators is that for the post-filtering mode the search is performed across a _field_ in an entire _index_. Whereas for the pre-filtering mode the search is performed only within a single _key_.

While there are many time/space tradeoffs possible for the pre-filtering case, it is proposed to handle the pre-filtering case the same as the post-filtering case only operating over an index that similarly constrained.
In other words, for each user-declared TEXT field there will be one text index constructed across all of the _keys_ as well a text index for each individual _key_.

As it turns out, this secondary per-key index is also useful to support key deletion as it contains exactly the words contained by the fields of the key and nothing else. This use-case helps drive the need for the Radix Tree and postings object to have representations optimized for very low numbers of contained objects.

## Defrag

The Postings object at the leaves of the prefix/suffix tree will likely vary substantially in size over time. This will create the opportunity for a lot of memory fragmentation. Implementating defrag is done by using the PathIterator to visit each node and defrag it.
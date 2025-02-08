# Text Index
The text index is logically a sequence of triples: (Word, Key, Position) _in lexical order_.
In addition to the standard CRUD operations the index efficiently supports iterating over sequences where the word shares a common prefix and 
optionally a common suffix. Other iterations sequences are optimized, for example within a word, 
it's efficient to move from one key to another next key without iterating over the interveneing positions -- typically in O(1) or worst case O(log #keys) time. 
The index is structured so that all iterations are always done in lexical order, allowing opertions like intersection and union that operate on multiple
 iteration sequences to perform merge-like operations in linear time. 
From this capability is constructed the various search operators: word search, phrase search, and fuzzy search.

This mapping is implemented as a two-level hierarchy of objects. At the top level is a radix tree which map words into a second object which is a container of
 (Key, Position) pairs. 
The use of the top-level radix tree allows efficient implementation of operations on subsets of the index that consist of words that have a common prefix.

In the historic terminology of text searching, the location of a word is called a Posting. For this application a posting would be the pair (Key, Position). 
The container of Posting objects for the same word is called a Postings.
The Postings object has an iterator that provides both an "advance to next position" and "advance to next key" operation efficiently.

Both the Postings and Radix tree implementations must adapt efficiently across a wide range in the number of items they contain. 
It's expected that both objects will have multiple internal representations to balance time/space efficiency. 
Likely the initial implementation will have two representstions. 
A space-efficient implementation with O(N) insert/delete/iterate times and a time-efficient implementation with O(1) or O(Log N) insert/delete/iterate times.

Like all of the Valkey search operators, the text search operators: word, phrase and fuzzy search must support both the pre-filtering and post-filtering modes when combined with vector search.
At the conceptual level, the only real difference between the pre- and post- filtering modes of the search operators is that for the post-filtering mode the search is performed across the entire corpus whereas for the pre-filtering mode the search is performed only on words within a single key.

While there are many time/space tradeoffs possible for the pre-filtering case, it is proposed to handle the pre-filtering case the same as the post-filtering case.
In other words, for each user-declared TEXT field there will be one text index constructed across all of the keys as well a text index index for each individual key (containing only that key). 

As it turns out, this secondary per-key index is also useful to support generic index maintenance. One major design problem for text indexing is how to remove enries in an index when a key is overwritten or deleted. What's needed is a list of the words that are contained in that key and the secondary per-key index has exactly that information in it. This usage is what drives the need to have a version of the Prefix tree and Postings object that are space-optimized AND have a small number of entries.


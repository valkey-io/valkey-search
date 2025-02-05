# Text Index
The text index is a multi-map from words to the locations of those words in the indexed corpus.
This mapping is implemented as a two-level hierarchy of objects. At the top level is a radix tree which map words into a second object which is a container of locations of words. 
The use of the top-level radix tree allows efficient implementation of operations on subsets of the index that consist of words that have a common prefix.

In the historic terminology of text searching, the location of a word is called a Posting and we will refer to the container as a Postings object. 
In addition to the standard CRUD operations a text index efficiently supports iterating over sequences of words (and their locations) that share a common prefix and optionally a common suffix. The index is structured so that iteration is always done in lexical order. This allows functions like intersection and union that operate by combining multiple iteration sequences to perform merge operations in time linear to the iteration length. 
From this capability is constructed the various search operators: word search, phrase search, and fuzzy search.

# Text Search Operators
Like all of the Valkey search operators, the text search operators: word, phrase and fuzzy search must support both the pre-filtering and post-filtering modes when combined with vector search. At the conceptual level, the only real difference between the pre- and post- filtering modes of the search operators is that for the post-filtering mode the search is performed across the entire corpus whereas for the pre-filtering mode the search is performed only on words within a single key.

While there are many time/space tradeoffs possible for the pre-filtering case, it is proposed to handle the pre-filtering case the same as the post-filtering case. In other words, there will be an index constructed across all of the keys as well as each text field of each key will itself have an instance of the text index. 

Some simple optimizations can be made, for example, no reason to create the per-key index if the containing index-schema doesn't have a vector field. 

Other optimizations, such as alternative internal object representations could also provide better space characteristics at the expense of time. For example, the per-key index might represent the radix tree in a slower, but more space-efficient format. Similarly for the Postings object. 





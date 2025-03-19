Text indexes are commonly referred to as inverted because they are not indexes from names to values, but rather from values to names. 
Within a running Valkey instance we can think of the text index as a collection of tuples and then reason about how these tuples are indexed for efficient operation.
Tuple members are:

* _Index_ -- The user-visible index (aka index-schema)
* _Field_ -- The TEXT field (attribute) definition with an index.
* _Word_ -- Query operators work on words.
* _Key_ -- The key containing this word, needed for result generation as well as combining with other search operators.
* _Position_ -- Location within a field (a word offset), needed for exact phrase matching.

There are some choices to make in how to index this information. There aren't any search operations that are keyed by _Position_, so this tuple element isn't a candidate for indexing. 

However, when looking across the various operations that the search module needs to perform it's clear that both _Key_-based and _Word_-based organizations are useful.

The ingestion engine wants a _Key_-based organization in order to efficiently locate tuples for removal (ingestion => remove old values then maybe insert new ones). It turns out that vector queries can also use a _Key_-based organization in some filtering modes.

Text query operations want a _Word_-based organization.
So the choice is of how to index the other members of a tuple: _index_, _field_, _key_ and _position_.
There are three different choices for _word_-based dictionary with very different time/space consequences.

One choice would be to have a single per-node _word_ dictionary. While this offers the best dictionary space efficiency, it will require each postings object to contain the remaining tuple entries: _index_, _field_, _key_ and _position_ for every word that's inserted. This prohibits taking advantage of the high rate of duplication in the _index_ and _field_ tuple members. A major problem with this choice is that in order to delete an _index_, you must crawl the entire _word_ dictionary. There are use-cases where index creation and deletion are fairly frequent. So this becomes a poor choice.

Another choice would be to organize a _word_ dictionary for each _index_. Now, the Postings object need only provide: _field_, _key_ and _position_ entries. This has the advantage of eliminating the highly redundant _index_ tuple member and the disadvantage of duplicating space in the words in the per-index _word_ dictionary. Data from the space-consumption page suggests that this is a good trade-off for larger dictionaries.

The last choice would be a per-_field_ word dictionary. Now the Postings object need only provide: key and position entries. Extending the pattern of the per-_index_ word dictionary, this has the advantage of eliminating both of the highly redundant tuple members: _index_ and _field_ with the disadvantage of even more potential duplication in the dictionary.

Having ruled out the per-node word dictionary, the choice between per-_index_ and per-_field_ is evaluated. The difference in the postings object size between these two choices need not be very large. In particular because the vast majority of indexes will likely have a small number of text fields only a very small number of bits would be required to represent a field in a per-_index_ and these could likely be combined with the _position_ field resulting in a per-_index_ posting that is negligably larger than the per-_field_ posting. Thus it's likely that the space savings of the per-_index_ word dictionary will dominate, making it the most space efficient choice.

Another reason to choose per-_index_ is that the query language of Redisearch is optimized for multi-field text searching. 
For example the query string: `(@t1|@t2):farkle` searches for the word `farkle` in two different text fields. The per-_index_ organization only requires only a single word lookup and a single postings traversal, while the per-_field_ organization would require two word lookups and two postings traversals. It should be noted that the Redisearch default for text queries is to search _all_ fields of type TEXT (which is different than the default for all other query operators that require a single field to be specified).

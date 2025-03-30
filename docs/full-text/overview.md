Text indexes are commonly referred to as inverted because they are not indexes from names to values, but rather from values to names.
Within a running Valkey instance we can think of the text index as a collection of tuples and then reason about how these tuples are indexed for efficient operation.
Tuple members are:

- _Schema_ -- The user-visible index (aka index-schema)
- _Field_ -- The TEXT field (aka attribute) definition with a _Schema_.
- _Word_ -- A lexical element. Query operators work on words.
- _Key_ -- The Valkey key containing this _Word_, needed for result generation as well as combining with other search operators.
- _Position_ -- Location within the _Field_ (stored as a _Word_ offset), needed for exact phrase matching. Future versions may extend the _Position_ to include the byte offset within the _Field_ to efficiently support highlighting.

There are some choices to make in how to index this information. There aren't any search operations that are keyed by _Position_, so this tuple element isn't a candidate for indexing.

However, when looking across the various operations that the search module needs to perform it's clear that both _Key_-based and _Word_-based organizations are useful.

The ingestion engine wants a _Key_-based organization in order to efficiently locate tuples for removal (ingestion => remove old values then maybe insert new ones). It turns out that vector queries can also use a _Key_-based organization in some filtering modes.

Text query operations want a _Word_-based organization.
So the choice is of how to index the other members of a tuple: _Schema_, _Field_, _Key_ and _Position_.
There are three different choices for _Word_-based dictionary with very different time/space consequences.

One choice would be to have a single per-node _Word_ dictionary. While this offers the best dictionary space efficiency, it will require each _Postings_ object to contain the remaining tuple entries: _Schema_, _Field_, _Key_ and _Position_ for every _Word_ present in the corpus. This prohibits taking advantage of the high rate of duplication in the _Schema_ and _Field_ tuple members.
A major problem with this choice is that in order to delete a _Schema_, you must crawl the entire _Word_ dictionary.
There are use-cases where Schema creation and deletion are fairly frequent. So this becomes a poor choice.

Another choice would be to organize a _Word_ dictionary for each _Schema_.
Now, the _Postings_ object need only provide: _Field_, _Key_ and _Position_ entries.
This has the advantage of eliminating the highly redundant _Schema_ tuple member and the disadvantage of duplicating space for words that appear in multiple Schemas as well as increasing the size of the _Posting_ object record the _Field_. More on this option below.

The last choice would be a per-_Field_ word dictionary. Now the _Postings_ object need only provide: _Key_ and _Position_ entries.
Extending the pattern of the per-_Schema_ word dictionary, this has the advantage of eliminating both of the highly redundant tuple members: _Schema_ and _Field_ with the disadvantage of duplicating words found in multiple fields in the corpus.

Having ruled out the per-node word dictionary, the choice between per-_Schema_ and per-_Field_ is evaluated. The difference in the _Postings_ object size between these two choices need not be very large.
In particular because the vast majority of indexes will likely have a small number of text fields only a very small number of bits would be required to represent a field and these could efficiently be combined with the _Position_ field resulting in a per-_Schema_ posting that is only an epsilon larger than the per-_Field_ posting.
Thus it's likely that the space savings of the per-_Schema_ word dictionary will dominate, making it the most space efficient choice.

Another reason to choose per-_Schema_ is that the query language of Redisearch is optimized for multi-field text searching.
For example the query string: `(@t1|@t2):entire` searches for the word `entire` in two different text fields. The per-_Schema_ organization only requires only a single word lookup and a single _Postings_ traversal, while the per-_Field_ organization would require two word lookups and two _Postings_ traversals.
It should be noted that the Redisearch default for text queries is to search _all_ fields of type TEXT (which is different than the default for all other query operators that require a single field to be specified).
Thus the per-_Schema_ organization is chosen.

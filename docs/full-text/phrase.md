# Exact Phrase Matching

The exact phrase search operator looks for sequences of words within the same field of one key. In the query language, the exact phrase consists of a sequence of words enclosed in double quotes. With `search.emulate-release` 1.3.0 or later, single quotes also delimit a phrase (see [COMPATIBILITY.md](../../COMPATIBILITY.md)).
Each word in a phrase is matched exactly; prefix, suffix and fuzzy operators are not supported inside a phrase.
The exact phrase search also has two parameters of metadata: _Slop_ and _Inorder_. The _Slop_ parameter is actually a maximum distance between words, i.e., with _Slop_ == 0, the words must be adjacent. With _Slop_ == 1, there can be up to 1 non-matching words between the matching words. The _Inorder_ parameter indicates whether the word specifiers must be found in the text field in the exact order as specified in the query or whether they can be found in any order within the constrains of _Slop_.

Iterators for each word specifier are constructed from the query and iterated. As each matching word is found, the corresponding _Postings_ object is then consulted and intersected to locate keys that contain all of the words. Once this key is located, then a position iterator for these keys is used to determine if the _Slop_ and _Inorder_ sequencing requirements are satisfied.

The implementation will need to provide some form of self-policing to ensure that the timeout requirements are honored as it's quite possible for these nested searches to explode combinatorially.

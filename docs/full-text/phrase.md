# Exact Phrase Matching

The exact phrase search operator looks for sequences of words within the same field of one key. In the query language, the exact phrase consists of a sequence of word specifiers.
Each specifier could be a wildcard match or a fuzzy match.
The exact phrase search also has two parameters of metadata: _slop_ and _inorder_. The _slop_ parameter is actually a maximum distance between the words, i.e., with _slop_ == 0, the words must be adjacent. With _slop_ == 1, there can be 0 or 1 non-matching words between the matching words. The _inorder_ parameter indicates whether the word specifiers must be found in the text field in the exact order as specified in the query or whether they can be found in any order.

Iterators for each word specifier are constructed from the query and iterated. As each matching word is found, the corresponding Postings object is then consulted and intersected to locate keys that contain all of the words. One this key is located, then a position iterator for these keys is used to determine if the _slop_ and _inorder_ sequencing requirements are satisfied.

The implementation will need to provide some form of self-policing to ensure that the timeout requirements are honored as it's quite possible for these nested search to explode combinatorially.

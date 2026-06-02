# Fuzzy Matching

There are many good blog posts on Levenshtein automata.  

https://julesjacobs.com/2015/06/17/disqus-levenshtein-simple-and-fast.html

https://fulmicoton.com/posts/levenshtein/

The bottom line is that the prefix tree representation of the data allows efficient fuzzy search for matches.
It's expected that building of the Levenstein automata is O(edit-distance * length-query-string) time and that the automata allows for efficient searching of a prefix-tree, because it can prune large subtree based on the evaluation of the sub-tree prefix.

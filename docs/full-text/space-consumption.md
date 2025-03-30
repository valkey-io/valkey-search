# Space Consumption

In this directory is the file "scrape.py" which is used to drive the following space consumption estimates.

This program randomly fetches pages from wikipedia and uses those to create a simulated corpus.
After ingestion a large number of pages statistics are gathered that should approximate the memory consumption that would used by this design.

This code treats each sentence as a key.
Each sentence is parsed into words and those words are subjected to stop word removal using the default list that's proposed in the full text search RFC.

The words are inserted into a simulation of the prefix Radix Tree and estimates of space for the Postings object are made.

Some estimates of different design options is made. The options are:

- Key8 - Assumes an 8 byte pointer for each keyname.
- Key4 - Assumes a 4 byte pointer for each keyname
- Tree8 -- Assumes an 8 byte pointer for each tree node
- Tree4 -- Assumes a 4 byte pointer for each tree node

Some selected example outputs are below:

```
Keys:12103 AvgKeySize:105.9 Words:217638/67232 AvgWord:4.8 Postings_space:1266/706KB Space:1734/1222/1016KB Space/Word:8.2/5.8 Space/Corpus:1.4/1.0/0.8

Keys:108325 AvgKeySize:108.2 Words:1982395/613720 AvgWord:4.8 Postings_space:11461/6399KB Space:13220/8581/7804KB Space/Word:6.8/4.4 Space/Corpus:1.2/0.7/0.7

Keys:246741 AvgKeySize:107.0 Words:4463767/1386123 AvgWord:4.8 Postings_space:25827/14416KB Space:28681/18234/16974KB Space/Word:6.6/4.2 Space/Corpus:1.1/0.7/0.7
```

An explanation of the outputs:

- Keys -The number of sentences processed.
- AvgKeySize - The average number of words in a sentence.
- Words - Total number of word occurrences in all sentences / total number of top words that appear.
- AvgWord - Average number of bytes in a word
- Postings_space - Estimate of total postings bytes in Key8/Key4
- Space - Estimate of total index space for Key8.Tree8 / Key4.Tree8 / Key4.Tree4 In KiloBytes
- Space/Word - Index bytes/word for Key8.Tree8 / Key4.Tree8 / Key4.Tree4
- Space/Corpus - Ratio of Index Space to Corpus for Key8.Tree8 / Key4.Tree8 / Key4.Tree4

Note that the Space/Corpus is the incremental space for the index itself, it doesn't include the original text.

For the Key8.Tree8 which is the simplest to develop, we should expect a full-text index to be approximately 110% of the original corpus, i.e. a bit better than twice the size. For the Key4.Tree8 that drops to 70%

It's clear from this simulation that the difference between Key8 and Key4 is quite substantial whereas the difference in space consumption between the Tree8 and Tree4 is relatively minor.

In actuality, this simulation is conservative because with the Key4 option it's relatively easy to expect most indexes to actually have less then 2^24 keys (16Million) which would only require 3 bytes per key reference, further reducing the overhead.

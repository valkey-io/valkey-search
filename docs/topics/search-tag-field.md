# Tag Fields

For both HASH and JSON index types, the incoming data is a single UTF-8 string.

On ingestion, each incoming field can contain multiple tags. The input field is split using the declared `SEPARATOR` character into separate tags.
Leading and trailing spaces are removed from each split field to form the final tag.

For query operations, both tag exact match and tag prefix match are supported. The tag match query operator

# Example Tag Queries

For these examples, the following index declaration and data set will be used.

```
valkey-cli FT.CREATE index SCHEMA color TAG
valkey-cli HSET key1 color blue
valkey-cli HSET key2 color black
valkey-cli HSET key3 color green
valkey-cli HSET key4 color beige
valkey-cli HSET key5 color "beige,green"
valkey-cli HSET key6 color "hello world, green is my heart"
```

## Simple Tag Query

```
valkey-cli FT.SEARCH index @color:{blue} RETURN 1 color
1) (integer) 1
2) "key1"
3) 1) "color"
   2) "blue"
```

## Multiple Tag Query

```
valkey-cli FT.SEARCH index "@color:{blue | black}" RETURN 1 color
1) (integer) 2
2) "key2"
3) 1) "color"
   2) "black"
4) "key1"
5) 1) "color"
   2) "blue"
```

## Prefix Tag Query

```
valkey-cli FT.SEARCH index @color:{b\*} RETURN 1 color
1) (integer) 4
2) "key2"
3) 1) "color"
   2) "black"
4) "key1"
5) 1) "color"
   2) "blue"
6) "key4"
7) 1) "color"
   2) "beige"
8) "key5"
9) 1) "color"
   2) "beige,green"
```

## Complex Tag Query

```
valkey-cli FT.SEARCH index @color:{b*|green} RETURN 1 color
1) (integer) 2
2) "key3"
3) 1) "color"
   2) "green"
4) "key5"
5) 1) "color"
   2) "beige,green"
```

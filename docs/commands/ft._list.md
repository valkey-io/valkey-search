Lists the currently defined indexes for the current data base.

```
FT._LIST [REGEX <pattern>]
```

- `REGEX <pattern>` (optional): Returns only indexes whose name partially matches the regular expression pattern. Use anchors such as `^` and `$` for exact-name matching. Patterns longer than `search.max-regex-pattern-bytes` (default 1024 bytes) are rejected.

Response

An array of strings. Each string is the name of an index in the current database. The index names are in no particular order.

Example

```
ft._list
1) index
2) products
3) users
4) transactions
```

```
ft._list REGEX ^(index|users)$
1) index
2) users
```

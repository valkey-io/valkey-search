Lists the currently defined indexes for the current data base.

```plaintext
FT._LIST [REGEX <pattern>]
```

- `REGEX <pattern>` (optional): Returns only indexes whose name partially matches the regular expression pattern. Use anchors such as `^` and `$` for exact-name matching.

Response

An array of strings. Each string is the name of an index in the current database. The index names are in no particular order.

Example

```text
ft._list
1) index
2) products
3) users
4) transactions
```

```text
ft._list REGEX ^index_.*
1) index_1
2) index_2
```

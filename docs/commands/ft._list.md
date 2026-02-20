Lists the currently defined indexes for the current data base.

```
FT._LIST
```

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
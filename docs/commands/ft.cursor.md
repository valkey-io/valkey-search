Reads more rows from, or deletes, a cursor created by the `WITHCURSOR` option of [`FT.AGGREGATE`](ft.aggregate.md) or [`FT.SEARCH`](ft.search.md).

```
FT.CURSOR READ <index-name> <cursor-id> [COUNT <count>]
FT.CURSOR DEL <index-name> <cursor-id>
```

- `<index-name>` (required): The name of an existing index. As in Redis, it need not be the index the cursor was created on.
- `<cursor-id>` (required): The cursor id returned by the command that created the cursor, or by a previous `FT.CURSOR READ`.
- `COUNT <count>` (optional): The maximum number of rows to return. It must be between 1 and `search.cursor-max-count`. The default is 1000.

A cursor holds the rows of a query result that have not yet been returned to the client, as of when the query ran: later changes to the data are not visible. A cursor belongs to the database it was created in: it can only be read or deleted by a connection whose currently selected database is that database. In cluster mode a cursor exists only on the node that executed the query.

A cursor is destroyed when its last row has been read, when it is deleted with `FT.CURSOR DEL`, or when it has not been read for longer than its `MAXIDLE` time. Reading a cursor restarts its idle time. If the index the cursor was created on has been dropped, `FT.CURSOR READ` returns an error and destroys the cursor.

`RESPONSE`

`FT.CURSOR READ` returns a two element array. The same format is used for cursors created by both `FT.AGGREGATE` and `FT.SEARCH`.

1. An array whose first element is the number of rows returned, followed by one element for each row. A row of an `FT.AGGREGATE` cursor is an array of field/value pairs. A row of an `FT.SEARCH` cursor is an array containing the elements for one key of a non-cursor `FT.SEARCH` response, i.e. the key name followed by the optional score, the optional sort key and the array of field/value pairs.
2. The cursor id to use for the next `FT.CURSOR READ`, or 0 if all rows have been returned, in which case the cursor has been destroyed.

`FT.CURSOR DEL` returns OK.

Errors:

- `Index with name '<index-name>' not found in database <db>`: the named index does not exist.
- `Cursor not found, id: <cursor-id>` / `Cursor does not exist`: there is no such cursor in this database.
- `The index was dropped while the cursor was idle`: the cursor's index was dropped (and possibly recreated) after the cursor was created.

# Example

```
> FT.AGGREGATE idx @price:[-inf inf] LOAD 1 @price SORTBY 2 @price ASC WITHCURSOR COUNT 2
1) 1) (integer) 2
   2) 1) "price"
      2) "10"
   3) 1) "price"
      2) "20"
2) (integer) 8481957688249417729
> FT.CURSOR READ idx 8481957688249417729 COUNT 2
1) 1) (integer) 2
   2) 1) "price"
      2) "30"
   3) 1) "price"
      2) "40"
2) (integer) 8481957688249417729
> FT.CURSOR READ idx 8481957688249417729
1) 1) (integer) 1
   2) 1) "price"
      2) "50"
2) (integer) 0
```

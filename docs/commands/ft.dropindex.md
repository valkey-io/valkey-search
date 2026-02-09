Drop the index created earlier by `FT.CREATE` command. It is an error if the index doesn't exist.

```
FT.DROPINDEX <index-name>
```

- `<index-name>` (required): The name of the index to delete.

`RESPONSE` OK or Error.

An OK response indicates that the index has been successfully deleted.

A first class of errors is directly associated with the command itself, for example a syntax error or an attempt to delete a non-existent index.

In CME a second class of errors is related to a failure to properly distribute the deletion across all of the nodes of the cluster. See [search index distribution](../topics/search-index-distribution.md) for more details.

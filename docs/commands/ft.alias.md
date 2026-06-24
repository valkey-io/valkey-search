Aliases provide alternative names for indexes. Once created, an alias can be used in place of the real index name in `FT.SEARCH`, `FT.AGGREGATE`, `FT.INFO`, and `FT.DROPINDEX` commands. Multiple aliases can point to the same index. Aliases are per-database and are persisted across server restarts.

Alias names must be non-empty strings that do not contain null bytes. An alias name must not match the name of an existing index (to avoid ambiguous resolution). The target index name must refer to a real index, not another alias (alias chaining is not supported).

# FT.ALIASADD

Creates a new alias that resolves to the specified index.

```
FT.ALIASADD <alias> <index-name>
```

- `<alias>` (required): The alias name to create. Must be non-empty and must not already exist as an alias or match an existing index name.
- `<index-name>` (required): The name of the target index. Must be an existing index (not an alias).

## Result

Returns `OK` on success or an error.

## Errors

- `Alias name cannot be empty` — the alias argument is an empty string.
- `Alias name must not contain null bytes` — the alias contains embedded null characters.
- `Alias already exists` — an alias with this name is already registered.
- `Alias collides with existing index name` — the alias matches a real index name.
- `Unknown index name or name is an alias` — the target does not exist or is itself an alias.
- `Index with name '<name>' not found in database <N>` — the target index does not exist.

## Example

```
FT.ALIASADD product_search my_product_index
OK
```

# FT.ALIASUPDATE

Creates a new alias or reassigns an existing alias to a different index (upsert semantics). If the alias does not exist, it is created. If it already exists, it is moved to point to the new target index. If the alias already points to the specified target, the command is a no-op.

```
FT.ALIASUPDATE <alias> <index-name>
```

- `<alias>` (required): The alias name to create or reassign. Must be non-empty and must not match an existing index name.
- `<index-name>` (required): The name of the target index. Must be an existing index (not an alias).

## Result

Returns `OK` on success or an error.

## Errors

- `Alias name cannot be empty` — the alias argument is an empty string.
- `Alias name must not contain null bytes` — the alias contains embedded null characters.
- `Alias collides with existing index name` — the alias matches a real index name.
- `Unknown index name or name is an alias` — the target does not exist or is itself an alias.
- `Index with name '<name>' not found in database <N>` — the target index does not exist.

## Example

```
FT.ALIASUPDATE product_search my_new_product_index
OK
```

# FT.ALIASDEL

Removes a previously created alias. After deletion, the alias name can be reused with `FT.ALIASADD`.

```
FT.ALIASDEL <alias>
```

- `<alias>` (required): The alias name to remove. Must be non-empty and must reference an existing alias (not an index name).

## Result

Returns `OK` on success or an error.

## Errors

- `Alias name cannot be empty` — the alias argument is an empty string.
- `Alias name must not contain null bytes` — the alias contains embedded null characters.
- `Alias does not exist` — no alias with this name is registered.

## Example

```
FT.ALIASDEL product_search
OK
```

# FT.ALIASLIST

Lists all aliases and their target indexes in the current database. Returns an empty array if no aliases exist.

```
FT.ALIASLIST
```

## Result

An array of alternating alias name and index name pairs, sorted lexicographically by alias name.

## Example

```
FT.ALIASLIST
1) "product_search"
2) "my_product_index"
3) "user_search"
4) "my_user_index"
```

# Behaviour Notes

- Dropping an index (`FT.DROPINDEX`) also removes all aliases that point to it.
- `FT.DROPINDEX` can be called with an alias name — it resolves to the underlying index and drops it (along with all of its aliases).
- `FT._LIST` returns only real index names; aliases are never included.
- `FT.INFO` can be queried via an alias and reports the aliases of the underlying index in its response.
- `FLUSHDB` removes all indexes and their aliases in the current database.
- `SWAPDB` moves aliases in lockstep with their indexes between databases.
- `MULTI/EXEC` and Lua scripts are not supported for alias commands in CME (Cluster Manager Enabled) mode.

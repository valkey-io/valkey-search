# Design document for FT.CURSOR facility.

This facility allows the output of a query operation: FT.AGGREGATE and (soon FT.HYBRID) to be saved internally and returned back to the client in pieces. A query generates a cursor object which can be incrementally consumed via the FT.CURSOR command.

When the WITHCURSOR option of FT.AGGREGATE is specified, a Cursor object is created. The Cursor object contains the output RecordSet and any other required metadata (Dialect, db number, Index name, destruction Timestamp, max Idle, etc.). [Note, if the command is executed while OOM and a CURSOR is requested, then the command is rejected with an OOM message]

Each cursor has a non-zero object id, the value selection is described below. There is a global table of all outstanding cursors. This table is triply-indexed: by id, by destruction timestamp, and by (DB, index name). Indexing by ID is O(1) [hash-based]. Indexing by timestamp is O(Log N) [std::multimap, whose iterators survive other inserts and erases]. The (DB, index name) index lets index removal -- FT.DROPINDEX, FLUSHDB/FLUSHALL, a replica full sync, an RDB load replacing an index, or a coordinator metadata drop -- discard exactly that index's cursors, from the single removal choke point in the SchemaManager. The per-DB entry is held by pointer, so SWAPDB moves an index's cursors along with it in O(1), by exchanging two pointers and their DB numbers.

A new slot in the cron callback chain is created. This callback scans the global table of cursors and destroys cursors whose destruction timestamp has passed. The actual destruction is done by passing the cursor object to a utility worker thread for destruction, subject to the search-result-background-cleanup configurable, which when off destroys it inline. The entry in the global table is destructed in the cron task. An INFO field provides the current # of cursors in the global table. A cursor holds a SearchParameters but its query is over, so cursor creation declares the operation terminated and the cursor stops counting towards the in-flight query count.

For both FT.CURSOR subcommands the named index must exist in the connection's current DB, but, matching Redis, it need not be the index the cursor was created on. The cursor's DB must match the connection's current DB (Redis does not check the DB, but Redis only supports indexes in DB 0; valkey-search indexes are per DB). Both subcommands also apply the key-prefix ACL check of the cursor's own index, the same check FT.SEARCH and FT.AGGREGATE apply to the query.

the FT.CURSOR DEL command locates the specific cursor ID number in the table, validates the matching DB and if found deletes it. If not found an error is generated.

the FT.CURSOR READ command locates the specific cursor in the table, validates the matching DB. If the index the cursor was created on has since been dropped (and possibly recreated), the cursor is destroyed and an error is returned. The destruction timestamp for the cursor is recomputed (using the stored maxidle) and the global map is updated accordingly. A result count is generated based on the min(currently remaining rows, maximum number of rows requested). The COUNT clause is optional, must be > 0 and is limited by the same configurable as on the WITHCURSOR clause. When absent, the cursor's current read size is used, which starts as the COUNT of the WITHCURSOR clause that created the cursor; when present it replaces that read size for subsequent reads of the cursor (matching Redis).

A result is generated using that count, that number of rows which are popped from the start of the object's RecordSet and either the original ID is reflected (if there are more rows left) or a 0 if the cursor is empty. If the cursor is empty, then it's destroyed and removed from the global table. The reply is a two element array, the same for cursors created by FT.AGGREGATE and FT.SEARCH: [0] is a count+1 sized array of the count followed by the rows, [1] is the continue cursor ID. A row of an FT.SEARCH cursor is an array holding the elements of one row of a non-cursor FT.SEARCH reply.

A cursor is a snapshot of the query output: changes to the data made after the query ran are not visible when the cursor is read.

The partial results setting has no effect on a query that requests a cursor: such a query always hands back the rows it gathered, so a timeout yields a cursor over a partial result rather than an error. For FT.AGGREGATE the pipeline stages still run to completion over those rows.

The syntax of the WITHCURSOR is the same for all three commands and it is accepted anywhere within these commands. The WITHCURSOR (case invariant) keywords is followed by two optional clauses COUNT and MAXIDLE both of which take integer values that have a minimum value of 1 and a configurable maximum value. If the COUNT clause is not specified, then it defaults to 1000. The default MAXIDLE is 300000. Default maximum count is 100000. Default maximum MAXIDLE is unlimited (max-pos)

The CURSOR id is an unsigned 64-bit integer. The upper half is formed by having a global 31-bit integer counter, which is incremented for each usage. It wraps around at 2^31, which keeps ids positive when replied as (signed) RESP integers. The low-order half is the CRC-32 hash of the run_id -- which is computed once at module load time. Each new cursor ID
is checked against the existing global table to ensure no accidental reuse. A cursor id of 0 is also not allowed.

# FT.SEARCH Modifications

The WITHCURSOR clause can appear anywhere after the query string. If multiple WITHCURSOR clauses occur the last is silently accepted. When present, the output of an FT.SEARCH becomes a three element array.

[0] element is the number of documents that match the query. This is the same as the first element of a regular FT.SEARCH array reply.
[1] An array of reply rows being returned. The same as the second element of a regular FT.SEARCH reply, i.e., an array of some number of records -- Maximum number is the record count value from the WITHCURSOR clause.
[2] The continue cursor. 0 if all rows were returned in this command. (No Cursor Object is generated in this case)

The cursor pages through the rows the query would have returned without WITHCURSOR, i.e., the LIMIT window (default LIMIT 0 10) or the first k results of a KNN query.

# FT.AGGREGATE Modifications

The WITHCURSOR clause can appear anywhere after the query string, i.e., before, concurrent or after the pipeline stage definitions. If multiple WITHCURSOR clauses occur the last is silently accepted.
The output matches the current definition which is a two element array.

[0] A count+1 sized array, where element [0] is the count and [1..count] are the reply rows. This matches a typical FT.AGGREGATE result
[1] The continue cursor ID. 0 if all rows were returned in this command. (No Cursor Object is generated in this case)

# Implementation

Refactor the existing main-thread reply generation functions for FT.SEARCh and FT.AGGREGATE to be free functions

Add FT._DEBUG SHOW_CURSORS, which returns an array with one [id, milliseconds until expiration] array per cursor.

Define the Cursor base class.

Define the CursorAggregateResult and CursorSearchResult classes derived from Cursor base class. Each should hold the necessary data to generate a response.

Setup the global cursor table.

Wire into the cron routine for updating the global cursor table.

Add to the INFO metric "num_cursors". This should be App visible.

For ft.Search. Output generation becomes bi-modal WITHCURSOR and without (the current path)

For FT.AGGREGATE. Output generation becomes BIMODAL: with and without cursor.

Update the command documentation for FT.SEARCH and FT.AGGREGATE.

Add the documentation for the FT.CURSOR command

# Unit tests

Good and bad syntax scanning cases for FT.CURSOR.

Expiration of 0, 1 and 2 cursors in a single call.

Cursor generation skips 0

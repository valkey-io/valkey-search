---
RFC: (PR number)
Status: (Change to Proposed when it's ready for review)
---

# Lua Scripting for FT.AGGREGATE

## Abstract

This proposal adds user-supplied [Lua](https://www.lua.org/) scripting to the Valkey Search
aggregation pipeline. Scripts are managed as named, server-side objects through a new
`FT.SCRIPT` command (which can `ADD`, `DEL`, `GET`, and `LIST` scripts) and live in a dedicated,
global script namespace that is independent of indexes, keyspaces, and databases. A new
`FT.AGGREGATE` pipeline stage, `SCRIPT`, invokes a previously registered script so that an
arbitrary, user-defined transformation can participate in an aggregation pipeline as a
first-class stage.

A `SCRIPT` stage receives the current ordered list of aggregation records as input and
produces a new ordered list of records as output, exactly like the built-in stages
(`APPLY`, `FILTER`, `SORTBY`, `LIMIT`, `GROUPBY`). The input and output lists are exposed to
Lua as deque-like objects supporting push/pop at both ends, and each record is exposed as a
mutable, unordered map of name/value pairs. An optional `SHARDABLE` keyword declares that the
script processes each input record independently, reserving the ability for a future
implementation to distribute (shard) the stage's computation across cluster nodes in a
map/reduce fashion. The `SHARDABLE` optimization is specified here but is **not** implemented
by this RFC; it is reserved for future work.

## Motivation

`FT.AGGREGATE` provides a fixed set of processing stages (`APPLY`, `FILTER`, `SORTBY`,
`LIMIT`, `GROUPBY`) and a fixed expression language with a closed set of built-in functions.
This covers the common cases, but real workloads frequently need transformations that the
built-in stages cannot express, for example:

- Computing a value that requires control flow, local state, or a lookup table that is
  awkward or impossible to express as a single `APPLY` expression.
- Reshaping records in ways that change the _number_ of records (fan-out / fan-in), which no
  current stage other than `GROUPBY`/`LIMIT` can do, and which none can do with arbitrary
  user logic.
- Custom scoring, re-ranking, deduplication, windowing, or enrichment logic that differs per
  application and does not justify a new built-in stage.

Today the only escape hatch is to transfer results to the client and post-process them there,
which moves large result sets over the network and gives up the locality of computing close
to the data. Adding a scripting stage lets users push arbitrary, sandboxed logic into the
pipeline where the data already is, while keeping the rest of the pipeline (and the
encapsulated `FT.SEARCH`) unchanged.

Lua is chosen because it is already a dependency of the Valkey ecosystem (it is the scripting
engine used by `EVAL`/`FUNCTION`), it is small, embeddable, and sandboxable, and users
familiar with Valkey scripting will find it natural.

## Terminology

In the context of this RFC:

- **Script**: A named blob of Lua source code registered with the server via `FT.SCRIPT ADD`.
- **Script namespace**: A single, global, flat namespace mapping script names to script
  bodies. It is independent of databases (it is not per-`SELECT`), independent of the index
  namespace, and independent of the Valkey keyspace.
- **`SCRIPT` stage**: An `FT.AGGREGATE` pipeline stage that invokes a named script.
- **Record**: One row of the aggregation pipeline; a mutable, unordered collection of
  name/value pairs (see _FT.AGGREGATE_). Maps to the existing `aggregate::Record`.
- **Record set**: The ordered list of records flowing between stages. Maps to the existing
  `aggregate::RecordSet`, which is a `std::deque<RecordPtr>`.
- **Locator**: A precomputed, high-performance handle to a specific named field within a
  record, valid for the duration of a stage. Maps to the integer position of a field in the
  existing `Record::fields_` vector.
- **Shardable**: A property of a `SCRIPT` stage asserting that its output for any input record
  depends only on that record, enabling per-record distribution across nodes.

## Design considerations

- **Consistency with existing stages.** A `SCRIPT` stage must behave like any other stage:
  consume the input record set, produce an output record set, and compose freely with the
  stages before and after it. The data model exposed to Lua must therefore be the existing
  record-set / record model, not a bespoke representation. A pipeline may contain **multiple
  `SCRIPT` stages**, in any positions and intermixed with the built-in stages; each is an
  independent invocation with its own fresh execution environment, and they share no state
  beyond the record set that flows from one to the next.

- **Zero-copy where possible.** The aggregation engine already represents the inter-stage
  result as a `std::deque<std::unique_ptr<Record>>` (`aggregate::RecordSet`) and each record
  as a vector of positionally-addressed values (`Record::fields_`) plus a small list of
  dynamically-named values (`Record::extra_fields_`). `Record::fields_` holds exactly the
  fields that are **known at command-parsing time** — those explicitly referenced anywhere in
  the `FT.AGGREGATE` command, i.e. any loaded field and any field named as an input or output
  of another stage — each assigned a fixed position. `Record::extra_fields_` holds all other
  fields, i.e. those not known until a script (or other stage) creates them at run time. The
  Lua bindings should wrap these in-place rather than serialize records into native Lua tables,
  both for performance and so that a script can mutate records cheaply.

- **Field addressing.** The primary addressing mode is by **string name**, which works
  uniformly for every field of a record regardless of how it is stored. On top of that, the
  design offers a _locator_ object as an optimization: fields that are **known at
  command-parsing time** (see _Zero-copy_ above) have a stable,
  known index in `Record::fields_`, and accessing them by string name on every touch would
  require a hash lookup per access. A locator resolves the name to its position once and then
  provides O(1) access for those parse-time-known fields, while still working (falling back to a
  by-name lookup) for any other field.

- **Safety and resource limits.** User scripts are untrusted code running inside the server
  process. The execution environment must be sandboxed (no file system, no network, no host
  OS access, no nondeterministic globals beyond what is explicitly provided) and must honor
  the existing `FT.AGGREGATE` `TIMEOUT` as well as memory limits. The number and total size of
  scripts must be bounded by configuration.

- **Cluster (CME) behavior.** The script namespace is global metadata, conceptually identical
  to index definitions. valkey-search already distributes index schemas to every node through
  `coordinator::MetadataManager` (see `MetadataManager::RegisterType`); scripts must use the
  same mechanism so that `FT.SCRIPT ADD`/`DEL` on any node is reflected on all nodes and a
  `SCRIPT` stage resolves identically everywhere. Like `FT.CREATE`/`FT.DROPINDEX`,
  `FT.SCRIPT ADD`/`DEL` are synchronous with respect to cluster consistency: the command does
  not complete successfully until the mutation has been propagated to full cluster consistency,
  and it fails (rather than returning early) if that consistency cannot be reached within the
  operation's timeout.

- **Forward compatibility for sharding.** The execution model must not bake in assumptions
  that would prevent a future map/reduce distribution of a `SHARDABLE` stage. In particular,
  the contract for a shardable script must forbid cross-record state, so that the engine is
  later free to partition the input record set, run the script on partitions on remote nodes,
  and concatenate the outputs.

### Comparison to similar features

- **RediSearch** does not expose user Lua inside the aggregation pipeline; it
  offers a fixed function set in `APPLY`/`FILTER` and `LOAD`-time field extraction. This
  proposal is strictly more expressive for the transformation step.
- **Valkey/Redis `EVAL` and `FUNCTION`** run Lua against the keyspace with a request/response
  shape. This proposal reuses the very same Valkey Lua interpreter and sandbox (see
  [Valkey Lua API](https://valkey.io/topics/lua-api/) and _Execution environment and isolation_),
  but differs in that the script operates on an in-flight aggregation record set rather than on
  keys, and is composed into a pipeline rather than invoked standalone. Crucially, unlike
  `EVAL`/`FUNCTION` scripts, an `FT.AGGREGATE` script **cannot call back into Valkey to execute
  commands and cannot read or mutate the key database** — `server.call`/`server.pcall` are
  removed from its `server` library, leaving no keyspace access at all. Its only effect is to
  transform the record set flowing through its stage.
- **Map/Reduce systems** (e.g. Hadoop/Spark) separate a per-record `map` from a cross-record
  `reduce`. The `SHARDABLE` keyword is the `map` analog: it is the user's assertion that the
  stage is a pure per-record map and may be distributed.

## Specification

### Script namespace

A new global namespace maps a script **name** to its Lua **body**. The namespace is:

- **Global**, not per-database: a script named `rerank` is the same object regardless of the
  client's selected database, and is visible to `FT.AGGREGATE` against any index in any
  database. (This mirrors how the feature is described as "independent of all other
  namespaces".)
- **Maintained exactly like index metadata.** The distribution, consistency, mutation,
  persistence, and replication of the script namespace follow the same template already
  established for index definitions (`FT.CREATE`/`FT.DROPINDEX` and the `SchemaManager` /
  `coordinator::MetadataManager` machinery). The namespace is registered as a new metadata type
  alongside `vs_index_schema`; `FT.SCRIPT ADD`/`DEL` create and delete metadata entries through
  the same path index-schema mutations use, and propagation across a CME cluster, fingerprinting
  and versioning, reconciliation on node join, RDB persistence, and replica consistency are all
  inherited from that existing mechanism rather than reinvented here. The remainder of this RFC
  assumes the reader is familiar with that machinery and only calls out script-specific
  differences.

#### System-defined limits

Two configurable limits bound the namespace (named consistently with existing options in
`valkey_search_options.cc`, e.g. `search.max-scripts` and `search.max-script-size`):

| Limit             | Meaning                                       | Default (proposed) | Min | Max     |
| ----------------- | --------------------------------------------- | ------------------ | --- | ------- |
| `max-scripts`     | Maximum number of scripts in the namespace    | 1024               | 0   | 16384   |
| `max-script-size` | Maximum size in bytes of a single script body | 65536              | 0   | 1048576 |

`FT.SCRIPT ADD` fails with an error when adding a script would exceed `max-scripts`, or when
the supplied body exceeds `max-script-size`. (Concrete default values are open for review; see
_Open questions_.)

### Commands

#### 1. `FT.SCRIPT`

Manages the global script namespace.

**Request**

```
FT.SCRIPT ADD  <name> <body>
FT.SCRIPT GET  <name>
FT.SCRIPT DEL  <name>
FT.SCRIPT LIST [<regex>]
```

- `FT.SCRIPT ADD <name> <body>` — Registers (or replaces) the script `<name>` with the Lua
  source `<body>`. The body is compiled and validated at registration time; a script that
  fails to compile is rejected and the namespace is unchanged. `ADD` of an existing name
  atomically replaces the prior body (and bumps its metadata version in CME). Fails if the
  body exceeds `max-script-size`, or if registering a new name would exceed `max-scripts`.

  **Response**: `+OK` on success; an error reply otherwise — compile error, limit exceeded
  (`max-scripts`/`max-script-size`), or, in CME, the cluster could not reach consistency within
  the timeout (cluster-not-consistent).

- `FT.SCRIPT GET <name>` — Returns the Lua source body registered under `<name>`, exactly as
  supplied to `ADD` (the stored source, not a recompiled form).

  **Response**: a bulk string containing the script body; an error reply if the name does not
  exist.

- `FT.SCRIPT DEL <name>` — Removes the named script from the namespace. Removal does not
  affect aggregations already in flight (which hold their resolved script for the duration of
  the command); subsequent `FT.AGGREGATE` invocations referencing the name fail at parse time.

  **Response**: `+OK` if a script was removed; an error reply if the name does not exist or,
  in CME, the cluster could not reach consistency within the timeout (cluster-not-consistent).

- `FT.SCRIPT LIST [<regex>]` — Returns the names of registered scripts. With no argument, all
  names are returned; with a `<regex>` argument, only names matching the regular expression are
  returned.

  **Response**: an array of bulk-string script names. (Bodies are retrieved individually with
  `FT.SCRIPT GET`, not via `LIST`.)

**Examples**

```
> FT.SCRIPT ADD topk "
    -- keep the K records with the highest @score, where K is the stage's first parameter
    local k = tonumber(...) or 10             -- first vararg; default to 10 if omitted
    local score = locator('score')            -- resolve the field name once
    local recs = {}
    while #input > 0 do
        recs[#recs + 1] = input:pop_front()   -- drain all input into a Lua array
    end
    table.sort(recs, function(x, y)           -- order by @score, descending
        return (x[score] or -math.huge) > (y[score] or -math.huge)
    end)
    for i = 1, math.min(k, #recs) do          -- emit the top K
        output:push_back(recs[i])
    end
  "
+OK

> FT.SCRIPT LIST
1) "topk"

> FT.SCRIPT GET topk
"\n    -- keep the K records with the highest @score, where K is the stage's first parameter\n    ..."

> FT.SCRIPT DEL topk
+OK
```

#### 2. `FT.AGGREGATE` — new `SCRIPT` stage

The `FT.AGGREGATE` stage grammar gains one production:

```
SCRIPT <name> <count> [param ...] [SHARDABLE]
```

- `<name>` must name a script that exists in the namespace at parse time; otherwise the
  command fails before execution begins.
- `<count>` is the number of stage parameters that follow, and may be `0`. The `count` `param`
  tokens after it are passed into the script as its chunk-level varargs (`...`), in order — the
  same way arguments reach the top of a Lua chunk. Each parameter is delivered to the script as
  a Lua **string**; a script converts as needed (e.g. `tonumber`). Like other `FT.AGGREGATE`
  argument values, a `param` may be given literally or as a `$name` reference, in which case the
  value bound to `name` by the command's `PARAMS` clause is substituted before the parameter is
  passed to the script — for example `SCRIPT topk 1 $k` with `PARAMS 2 k 5` passes the string
  `5` as the first vararg.
- `SHARDABLE` is an optional assertion by the caller that the script is a pure per-record map
  (see _Sharding_). It is accepted and validated syntactically by this RFC, and recorded on
  the stage, but does not change execution behavior in this implementation: a `SHARDABLE`
  stage runs identically to a non-`SHARDABLE` stage (locally, over the whole record set). The
  keyword exists so the capability is reserved and so that existing scripts authored with it
  begin benefiting automatically once distribution is implemented.

Updated stage list (additions in **bold**), extending the grammar in `ft-aggregate.md`:

```
FILTER  expression
APPLY   expression AS name
LIMIT   offset num
GROUPBY count property [property ...] [REDUCE ...]
SORTBY  count [ property ASC | DESC ... ] [MAX num]
SCRIPT  name count [param ...] [SHARDABLE]
```

**Example**

Invoking the `topk` script defined above, passing `k = 5` as the stage's single parameter (the
script reads it as its first vararg, `...`):

```
FT.AGGREGATE idx "*"
   LOAD 1 @score
   SCRIPT topk 1 5
```

This returns the 5 highest-scoring records. In `SCRIPT topk 1 5`, the `1` is the parameter
count and `5` is that one parameter, delivered to the script as its first vararg.

### Execution model

A `SCRIPT` stage implements the existing `aggregate::Stage` interface
(`Execute(RecordSet& records)`). When executed it:

1. Acquires a Lua interpreter from a per-thread pool and loads the (pre-compiled) script
   chunk for `<name>`.
2. Binds two deque-like objects into the script's environment:
   - `input` — the incoming record set (the `RecordSet&` passed to `Execute`).
   - `output` — a fresh, empty record set that the script fills.
     The stage's `count` parameters are passed to the chunk as its varargs (`...`), so the script
     can read them with `local a, b = ...` or `local args = {...}`.
3. Runs the script body. By convention the script drains records from `input`, transforms
   them, and pushes results onto `output`. (For a pure in-place mutation the script may move
   each record straight from `input` to `output`; see the `topk` example above.)
4. On completion, the contents of `output` replace the stage's result and become the input to
   the next stage. Any records the script left in `input` are discarded. Errors raised by the
   script (`error(...)`, runtime faults, exceeding `TIMEOUT` or memory limits) abort the
   aggregation with an error reply.

This input → output substitution is what makes the stage able to grow or shrink the record
count, exactly as `GROUPBY`, `LIMIT`, and `FILTER` already do. (`FILTER` likewise changes the
number of records by dropping those that fail its predicate, and because each drop/keep decision
depends only on the record being tested, `FILTER` is itself effectively shardable — the same
property the `SHARDABLE` keyword lets a `SCRIPT` stage assert.)

#### Execution environment and isolation

A script executes inside a **per-command execution environment**. The environment is created
when the `FT.AGGREGATE` command begins executing and is torn down when the command completes;
it is private to that single command invocation. Concretely:

- The environment is **not** shared across commands, across clients, or across the stages of
  unrelated aggregations. Two concurrent `FT.AGGREGATE` commands — even ones invoking the same
  script — get independent environments and cannot observe one another. Nothing a script does
  survives the command that ran it.
- A script can access **only** what the engine explicitly places into its environment for that
  command: the `input` and `output` record-set objects, the records and locators derived from
  them, the stage's own parameters (its varargs, `...`), the helper functions provided by the
  engine (below), and the script's own local variables. It has **no** access to anything outside
  this environment — no Valkey
  keyspace, no other databases, no indexes, no other scripts, no server configuration, no
  other commands' state, and no host facilities (file system, network, processes, clock,
  environment variables, randomness).
- **The sandbox is the one Valkey already defines for scripting.** This feature does not invent
  a new Lua environment; it reuses mainline Valkey's embedded Lua sandbox as documented in the
  [Valkey Lua API](https://valkey.io/topics/lua-api/). Specifically, a `SCRIPT` stage runs on
  **the same Lua interpreter**, with **the same preloaded libraries** (the standard `string`,
  `table`, `math`, `struct`, `cjson`, `cmsgpack`, `bit`, and the limited `os` subset Valkey
  exposes), and **the same restrictions on global variables** (global protection — scripts may
  not create or rely on arbitrary globals, and `require`/`loadfile`/`dofile` and similar escapes
  are unavailable) that Valkey applies to `EVAL`/`FUNCTION` scripts.
- **The `server` library is included, but trimmed.** The Valkey `server` library is present, with three deliberate differences from `EVAL`:
  - **No command execution.** `server.call` and `server.pcall` — and every function tied to
    command execution and replication (e.g. `server.error_reply`/`server.status_reply`,
    `server.setresp`, `server.set_repl`, `server.replicate_commands`) — are **removed**. This is
    what enforces the "cannot execute Valkey commands or touch the key database" guarantee.
  - **No debugging support.** The Lua debugging hooks (e.g. `server.breakpoint`, `server.debug`,
    and the `debug` library) are **removed**.
  - **Logging is retained.** `server.log` and its associated log-level constants
    (`server.LOG_DEBUG`/`LOG_VERBOSE`/`LOG_NOTICE`/`LOG_WARNING`) remain available so a script
    can emit diagnostics to the server log.

Because the only data a script can reach is the record set it is handed (plus the stage's
varargs, which are constant for the whole stage), a script is a pure transformation of (its
input record set, the stage parameters, the provided functions) → (its output record set). This
isolation is what later permits a `SHARDABLE` stage to be relocated to another node: the
parameters are identical on every node, so by construction there is nothing in the environment
for it to depend on except the records it processes.

#### Provided functions

The sandboxed Lua standard library already covers most of the `FT.AGGREGATE` expression
engine's function set — `math.log`/`math.abs`/`math.sqrt` and `string.upper`/`string.lower`/
`string.sub` are direct equivalents of the engine's numeric and string functions — so no
parallel helper library is needed for those.

**The `vector` table.** Because vector fields are delivered to a script as binary strings (see
_Field value types_), the engine provides a built-in Lua table named `vector` whose functions
let a script compare and convert vectors without decoding the bytes by hand:

| Function                        | Returns                                                                   |
| ------------------------------- | ------------------------------------------------------------------------- |
| `vector.l2(encoding, a, b)`     | Euclidean (L2) distance between vectors `a` and `b`.                      |
| `vector.cosine(encoding, a, b)` | Cosine distance between vectors `a` and `b`.                              |
| `vector.ip(encoding, a, b)`     | Inner (dot) product of vectors `a` and `b`.                               |
| `vector.totable(encoding, v)`   | A Lua table of numbers (1-based) holding the components of vector `v`.    |
| `vector.fromtable(encoding, t)` | A vector (binary string) built from the numbers in the 1-based table `t`. |

- `encoding` selects how the bytes are interpreted: one of `'FP32'`, `'FP16'`, or `'BFLOAT16'`.
  For the distance functions both operands are interpreted under the same encoding.
- The vector operands (`a`, `b`, `v`) are Lua strings whose bytes are the vector components in
  that encoding (the same native-endian binary representation a vector field reads as). Typically
  they come straight from a vector-typed field of a record.
- `vector.l2`/`vector.cosine`/`vector.ip` each return a scalar Lua `number`.
- `vector.totable` decodes a vector into a plain Lua sequence (a table indexed `1..n` with no
  gaps, following Lua's standard 1-based convention), each element a Lua `number`, so a script
  can inspect or manipulate individual components.
- `vector.fromtable` is the inverse: given an `encoding` and a 1-based table of numbers, it
  encodes the components into a vector (binary string) under that encoding — suitable for storing
  back into a vector-typed field or passing to another `vector` function. `totable` and
  `fromtable` round-trip under the same encoding (subject to the precision of `FP16`/`BFLOAT16`).
- **Errors are raised as standard Lua errors.** If the inputs are invalid for any reason —
  operand lengths that are not equal (distance functions), a length that is not a whole number of
  elements for the encoding, a table element that is not a number (`fromtable`), an unknown
  `encoding`, or otherwise undecodable bytes — the function raises a Lua error (i.e. it calls
  `error(...)`), exactly as a malformed call to a standard library function would. An uncaught
  error aborts the stage and fails the `FT.AGGREGATE` command; a script that wants to tolerate
  bad input can wrap the call in `pcall`.

Any further engine-provided helpers (names, signatures, and the table they live under) are
**TBD** and will be specified in the implementing PR; the only constraints are that each must be
deterministic and sandbox-safe.

#### Object model exposed to Lua

The objects below are implemented as Lua userdata with metatables that proxy directly to the
underlying C++ objects (no copying of record contents into Lua tables).

**Record-set object** (`input`, `output`) — a deque of records.

| Operation            | Semantics                                               |
| -------------------- | ------------------------------------------------------- |
| `rs:push_front(rec)` | Insert `rec` at the front.                              |
| `rs:push_back(rec)`  | Insert `rec` at the rear.                               |
| `rs:pop_front()`     | Remove and return the front record (or `nil` if empty). |
| `rs:pop_back()`      | Remove and return the rear record (or `nil` if empty).  |
| `#rs`                | Number of records currently in the set.                 |
| `rs:new_record()`    | Construct a new, empty record owned by the engine.      |

These map one-to-one onto `RecordSet`'s existing `push_front`/`push_back`/`pop_front`/
`pop_back` and `size()`. A record returned by `pop_*` is owned by the script until it is
pushed onto a record set; pushing transfers ownership (matching the `RecordPtr` move
semantics already used by `RecordSet`).

**Record object** — a mutable, unordered map of name/value pairs (analogous to
`std::unordered_map`, meaning that `pairs(rec)` visits the fields in an unspecified order). Backed by
`Record::fields_` (named, positional fields known at command parsing time) and `Record::extra_fields_` (dynamically named
fields). This split is an implementation detail: **string addressing works uniformly for every
field of the record**, regardless of which backing store holds it. The locator (below) is an
optional optimization layered on top, not a separate set of addressable fields.

| Operation           | Semantics                                                                                                                                                |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rec[name]`         | Read the value of field `name` by string key. Works for **any** field (whether it lives in `fields_` or `extra_fields_`). Returns `nil` if absent.       |
| `rec[name] = value` | Create or update field `name` by string key (any field). Setting `nil` deletes the field.                                                                |
| `rec[loc]`          | Read via a _locator_ (see below). Works for any field; O(1) when the locator resolves to a `fields_` position, otherwise falls back to a by-name lookup. |
| `rec[loc] = value`  | Write via a locator (same performance characteristics as the read).                                                                                      |
| `rec:delete(name)`  | Explicitly delete a field (equivalent to `rec[name] = nil`).                                                                                             |
| `rec:has(name)`     | `true` if the field exists.                                                                                                                              |
| `pairs(rec)`        | Iterate existing name/value pairs in unspecified order (read-only view during iteration).                                                                |
| `#rec`              | Number of fields.                                                                                                                                        |

**Field value types.** A field value is an `expr::Value`. The mapping between an `expr::Value`
and the Lua value a script sees is:

| `expr::Value` type | Lua type seen by the script     | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------ | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Nil`              | `nil`                           | A missing field also reads as `nil`. Assigning `nil` deletes the field.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Number (`double`)  | `number`                        | Values round-trip as Lua 64-bit floats.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `Bool`             | `boolean`                       | Maps directly to Lua `true`/`false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| String             | `string`                        | Byte-exact; strings are binary-safe (not assumed UTF-8).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Vector             | `string` (binary)               | A vector field is delivered as a Lua **string** whose bytes are the vector's components encoded as binary floating-point in the **native byte order of the CPU executing the script**. A script that interprets the bytes directly (e.g. via `string.unpack`) must assume host endianness; more conveniently, `vector.totable`/`vector.fromtable` (see _The `vector` table_) decode and re-encode a vector to and from a table of Lua numbers. Writing a vector-typed field back is symmetric: a string of correctly-sized native-endian floats. |
| Array              | `table` with dense integer keys | The `Array` value type (to be added; it is the value produced by the `TOLIST` reducer/function — see _Open questions_ / Appendix) is presented as a Lua table indexed `1..n` with no gaps, i.e. a Lua sequence (`#t` is its length). Element types are the scalar mappings above, applied recursively. Constructing an `Array` to store into a field is done by building such a dense-integer-keyed table.                                                                                                                                       |

Writing a field with a Lua type that isn't in the table above
(e.g. a function, a coroutine, etc.), raises an error.

**Locator object** — a reusable handle to a named field that _can_ provide high-performance
access.

A locator is created once (typically at the top of the script) and then reused for every
record in the loop:

```
local loc = locator("score")     -- resolve the field name once
while #input > 0 do
    local rec = input:pop_front()
    rec[loc] = rec[loc] * 2      -- fast field access when 'score' maps to fields_
    output:push_back(rec)
end
```

A locator may be created for **any** field name — there is no error case based on the field's
backing store. What varies is the cost of using it:

- When the name resolves to a field with a stable, known position in `Record::fields_`, the
  locator captures that position and indexing a record by the locator reads/writes
  `fields_[position]` directly — O(1), with no per-touch hash lookup. These are precisely the
  fields the parser already assigns a fixed position via
  `AggregateParameters::AddRecordAttribute` (tracked in `record_indexes_by_alias_` /
  `record_indexes_by_identifier_`), namely:
  - a named index field that is explicitly loaded — including its **alias** — via `LOAD`
    (alias loading is being implemented in a separate PR), and
  - a field created as the output of an `APPLY` stage or a reducer.

- For any other name (a dynamically-named field living in `extra_fields_`, or a field not
  present at parse time), the locator still works but degrades to the same by-name lookup that
  string addressing performs. It offers no speedup in that case, but it is not an error.

In other words, the locator is purely an optimization hint; correctness never depends on
whether a given name happens to resolve to `fields_`. String addressing (`rec[name]`) remains
available for every field at all times.

#### Worked example: summarize a record set into one record

The following script demonstrates the full object model: it **consumes the entire input record
set**, mixes **locator** access (for the loaded `score` field) with **string** access and field
**enumeration with type checking**, and emits a **single output record** whose fields are
aggregate characteristics of the input.

The summary record it produces has three fields:

- `num_records` — the number of input records consumed.
- `max_score` — the largest value of the `score` field across all records, read through a
  locator (the fast path, since `score` is loaded and therefore lives in `fields_`).
- `longest_string` — the byte length of the longest _string-typed_ field found anywhere in the
  input, discovered by enumerating each record's fields with `pairs` and checking the Lua type
  of each value.

```
> FT.SCRIPT ADD summarize "
    local score = locator('score')      -- fast path: 'score' is a loaded field (fields_)
    local count = 0
    local max_score = -math.huge
    local longest = 0

    while #input > 0 do
        local rec = input:pop_front()
        count = count + 1

        -- locator access to a known numeric field
        local s = rec[score]
        if type(s) == 'number' and s > max_score then
            max_score = s
        end

        -- enumerate every field by name; type-check to find string fields
        for name, value in pairs(rec) do
            if type(value) == 'string' then
                local len = #value          -- byte length of the string value
                if len > longest then
                    longest = len
                end
            end
        end
    end

    -- build a single summary record for the output
    local out = output:new_record()
    out['num_records']    = count
    out['max_score']      = (count > 0) and max_score or nil
    out['longest_string'] = longest
    output:push_back(out)
  "
+OK
```

Used in a pipeline, this collapses any result set into one summary row:

```
FT.AGGREGATE idx "*"
   LOAD 2 @score @title
   SCRIPT summarize 0
```

Because the script produces its single output from characteristics computed across _all_ input
records (`num_records`, and the maxima), it is inherently cross-record and is **not** marked
`SHARDABLE` — the per-record independence rule does not hold for it.

#### Worked example: per-record vector distance and rerank

This script annotates each record with the cosine distance between two of its own vector fields
— `title_vec` (a loaded field, addressed through a locator) and `body_vec` (addressed by
string) — then drops any record for which the distance could not be computed. It uses the
built-in `vector.cosine` distance function with the `FP32` encoding, catching the Lua error that
`vector.cosine` raises on bad input via `pcall`, and it processes each record **independently**
of every other record, so it is a legitimate `SHARDABLE` stage.

```
> FT.SCRIPT ADD title_body_sim "
    local tvec = locator('title_vec')   -- fast path: 'title_vec' is a loaded field
    local out = output

    while #input > 0 do
        local rec = input:pop_front()

        local a = rec[tvec]              -- vector field -> binary string
        local b = rec['body_vec']        -- string-addressed vector field

        -- cosine distance between the two FP32 vectors; vector.cosine() raises on bad
        -- input, so wrap it in pcall and keep only the records that computed cleanly
        local ok, d = pcall(vector.cosine, 'FP32', a, b)

        if ok then
            rec['sim'] = d               -- annotate with the computed distance
            out:push_back(rec)
        end
        -- records that errored (mismatched size, missing field, etc.) are dropped
    end
  "
+OK
```

Used in a pipeline that sorts the survivors by the computed similarity:

```
FT.AGGREGATE idx "*"
   LOAD 2 @title_vec @body_vec
   SCRIPT title_body_sim 0 SHARDABLE
   SORTBY 2 @sim ASC
   LIMIT 0 10
```

The output for each input record depends only on that record's own two vector fields, so the
`SHARDABLE` annotation is honest: a future implementation is free to run this stage on remote
nodes over partitions of the record set. (A missing or malformed field makes `vector.cosine`
raise an error, which the `pcall` catches and turns into a dropped record, so the script is
robust to records that lack one of the vectors.)

### Sharding (reserved, not implemented)

The `SHARDABLE` keyword declares that the script satisfies the **independence rule**: the
output records produced for an input record are a function of _that input record alone_ and do
not depend on, or communicate through, any other input record or any state that persists
across records. Equivalently, partitioning the input record set into disjoint subsets, running
the script independently on each subset, and concatenating the outputs must produce a result
equivalent to running the script once over the whole set (order within the concatenation
follows partition order).

This is the `map` half of map/reduce. When a stage is marked `SHARDABLE`, a future
implementation may, in cluster mode:

1. Partition the input record set (for example by the node that produced each record, or by an
   arbitrary even split).
2. Ship each partition to a remote node and run the script there ("map" close to the data).
3. Stream the per-partition outputs back and concatenate them, feeding the next stage.

A non-`SHARDABLE` stage (the default) makes no such promise and must always run over the
complete record set on a single node.

Because the optimization is unimplemented here, the engine treats `SHARDABLE` and
non-`SHARDABLE` stages identically at runtime. The keyword is parsed, validated, recorded on
the stage object, and round-tripped (e.g. in `Dump`/explain output) so that:

- scripts can be authored and deployed with the annotation today, and
- enabling distribution later requires no change to user commands.

Violating the independence rule while asserting `SHARDABLE` yields undefined results once
distribution is implemented; until then it has no effect. The engine cannot in general verify
the rule, so it is the caller's contract (mirroring how `EVAL` scripts in cluster mode are
contractually required to touch only keys in one slot).

### Authentication and Authorization

- **A script sees no data that the client could not already obtain.** A `SCRIPT` stage operates
  solely on the record set flowing through the very `FT.AGGREGATE` command the client issued —
  records derived from the encapsulated `FT.SEARCH` over an index the client named, filtered and
  projected by exactly the same index-definition visibility and `LOAD`/`RETURN` rules that
  already govern that command. Combined with the per-command sandbox (a script cannot reach the
  keyspace, other indexes, or anything outside its environment — see _Execution environment and
  isolation_), this means scripting exposes no data path that plain `FT.AGGREGATE` does not.
  **No additional data-access security is therefore required**; the existing `FT.AGGREGATE` and
  index-definition authorization is sufficient and unchanged.
- `FT.AGGREGATE` (and therefore the `SCRIPT` stage) keeps the same authorization as today
  (the `READ`/`SLOW`/`SEARCH` ACL categories already declared in `ft.aggregate.json`).
- `FT.SCRIPT` mutates global server state and should be treated as an administrative,
  write-class command. It is proposed to carry the `WRITE`, `SLOW`, and `SEARCH` ACL
  categories (with `FT.SCRIPT LIST` being `READ`), so that the ability to register code can be
  restricted via ACLs independently of the ability to run aggregations.
- Registering a script is registering executable code; deployments that disallow `EVAL`-style
  scripting should be able to disable this feature. See _Configuration_.
- **There are no per-script ACL or visibility restrictions.** A script is not an ACL-protected
  resource and has no owner. Any script in the namespace can be invoked (via a `SCRIPT` stage),
  inspected (`FT.SCRIPT GET`), and listed (`FT.SCRIPT LIST`) by any user who is permitted to
  run the corresponding command — there is no notion of a script being visible to some users
  but not others. Access control exists only at the _command_ granularity (who may run
  `FT.SCRIPT`/`FT.AGGREGATE` at all), never at the _individual-script_ granularity. The script
  namespace is global and uniformly accessible, exactly like the index namespace.

### Configuration

| Option                     | Type   | Default | Purpose                                                                        |
| -------------------------- | ------ | ------- | ------------------------------------------------------------------------------ |
| `search.scripting-enabled` | bool   | `yes`   | Master switch. When disabled, `FT.SCRIPT` and the `SCRIPT` stage are rejected. |
| `search.max-scripts`       | number | 1024    | Maximum number of scripts in the namespace.                                    |
| `search.max-script-size`   | number | 65536   | Maximum size (bytes) of a single script body.                                  |

Options follow the existing pattern in `valkey_search_options.cc` (default/min/max bounded
numeric options, boolean toggles).

### RDB, Replication, and Cluster mode

These are inherited wholesale from the index-metadata maintenance path; see _Script namespace_.
The script namespace is registered as a new metadata type alongside `vs_index_schema`, so RDB
persistence (a versioned aux section storing each script's name and body), replication of
`FT.SCRIPT ADD`/`DEL` to replicas, and CME distribution (fingerprinting, versioning, cluster-bus
broadcast, and reconciliation on node join) all reuse the existing `SchemaManager` /
`coordinator::MetadataManager` machinery. No new top-level data type and no new cluster-bus
message types are introduced.

The only script-specific note is behavior when a stored script fails to recompile on RDB load
under the running server version: the proposal is to report and skip it rather than abort the
load (open for review).

### Module API

No new public module API is required. Internally the feature adds:

- a script-registry component owning the name→body map and the compiled-chunk cache, wired
  into `MetadataManager` (CME) and the RDB aux save/load callbacks;
- a `ScriptStage : aggregate::Stage` implementation;
- a thin Lua-binding layer wrapping `RecordSet`, `Record`, and the locator over `expr::Value`.

### Dependencies

No new dependency is introduced. This feature reuses mainline Valkey's existing embedded Lua
interpreter and sandbox — the same one `EVAL`/`FUNCTION` use, documented in the
[Valkey Lua API](https://valkey.io/topics/lua-api/) — including its preloaded libraries and
global-variable restrictions (see _Execution environment and isolation_). The only additions are
the engine-provided helpers this RFC defines (the `vector` table) and the trimming of the
`server` library (removal of command execution, replication, and debugging functions while
retaining logging); no new third-party library is vendored.

### Networking

No changes to the RESP protocol or client interaction model. `FT.SCRIPT` is an ordinary
command; the `SCRIPT` stage is internal to `FT.AGGREGATE`.

### Observability

Proposed metrics/stats:

- `search_scripts_count` — number of registered scripts.
- `search_scripts_bytes` — total bytes of registered script bodies.
- `search_script_stage_executions_total` — count of `SCRIPT` stage executions.
- `search_script_stage_errors_total` — count of `SCRIPT` stage failures (compile/runtime/
  timeout/memory).
- per-command timing for `SCRIPT` stages surfaced through existing `FT.AGGREGATE` profiling.

### Testing

- **Unit tests** for `FT.SCRIPT ADD/GET/DEL/LIST`: registration, replacement, byte-exact
  `GET` round-trip of the stored body, deletion, limit enforcement (`max-scripts`,
  `max-script-size`), compile-error rejection, and `scripting-enabled` gating.
- **Unit tests** for the object model: deque push/pop at both ends on `input`/`output`; record
  read/update/delete/add; iteration via `pairs`; string vs. locator addressing equivalence;
  locator fast-path (resolves to `fields_`) vs. by-name fallback (non-positional fields); type
  marshaling for every value type (`nil`/number/boolean/string,
  vector-as-native-endian-binary-string, and `Array`-as-dense-integer-keyed-table, including
  recursive element marshaling) and rejection of unsupported Lua types (functions, coroutines,
  sparse/string-keyed tables where an `Array` is expected).
- **Unit tests** for the `vector` table: distance functions (`vector.l2`/`vector.cosine`/
  `vector.ip`) give correct values for each encoding (`FP32`/`FP16`/`BFLOAT16`) against known
  references; `vector.totable` decodes to a correct 1-based table of numbers and
  `vector.fromtable` round-trips back to the original bytes (within `FP16`/`BFLOAT16` precision);
  and every error case (mismatched lengths, non-element-aligned length, non-number table element,
  unknown encoding, undecodable bytes) raises a Lua error catchable via `pcall`.
- **Parsing/stage tests** for `SCRIPT name count [param ...]`: `count` of `0` (no varargs),
  `count` > 0 delivered to the chunk as `...` in order and as strings, `count` mismatch against
  the actual token run rejected at parse time, `$name` parameters substituted from the command's
  `PARAMS` clause, and correct placement of an optional trailing `SHARDABLE`.
- **Stage tests** verifying a `SCRIPT` stage composes with `LOAD`, `APPLY`, `FILTER`,
  `SORTBY`, `LIMIT`, `GROUPBY`, and with **other `SCRIPT` stages** (multiple independent scripts
  in one pipeline), including stages that grow and shrink the record count.
- **`FT.SCRIPT LIST` tests**: no-argument lists all names; a `<regex>` argument returns only
  matching names (including no-match and match-all patterns).
- **Resource tests**: `TIMEOUT`/memory enforcement aborts cleanly; runtime `error()` produces
  a proper error reply; sandbox escapes (`io`/`os`/`require`) are unavailable.
- **Integration / cluster tests** (`build.sh --run-integration-tests=test_...`): script
  namespace propagation across CME nodes via the metadata manager, RDB save/load round-trip,
  replica consistency, and that `FT.SCRIPT ADD`/`DEL` block until full cluster consistency and
  return a cluster-not-consistent error on timeout.
- **Sharding contract tests**: confirm that `SHARDABLE` is parsed and recorded but runs
  identically to the non-shardable path (regression guard for when distribution lands).

## Open questions

- **Default limits.** Are `max-scripts = 1024` and `max-script-size = 64 KiB` reasonable
  defaults, and should they be hard-capped lower/higher?
- **Provided functions.** The concrete set of engine-provided helper functions, their
  signatures, and the table/namespace they are exposed under is TBD (see _Provided functions_).
- **RDB version skew.** On load, should a script that fails to recompile under the current
  server version skip-and-warn (proposed) or abort the load?

## Appendix

- [Valkey Lua API](https://valkey.io/topics/lua-api/) — the existing mainline Valkey Lua
  scripting environment (interpreter, preloaded libraries, global restrictions, and `server`
  library) that this feature reuses.
- `rfc/ft-aggregate.md` — the base `FT.AGGREGATE` design this stage extends.
- `rfc/TEMPLATE.md` — RFC structure.
- Existing implementation touch points: `src/commands/ft_aggregate_parser.h`
  (`Stage`, `AggregateParameters::AddRecordAttribute`, `record_indexes_by_alias_`),
  `src/commands/ft_aggregate_exec.h` (`Record`, `RecordSet` deque),
  `src/coordinator/metadata_manager.h` (`MetadataManager::RegisterType`, global metadata
  distribution), `src/schema_manager.h` (`kSchemaManagerMetadataTypeName` precedent),
  `src/valkey_search_options.cc` (bounded configuration option pattern).
- Related: separate PR adding `LOAD` alias support, which establishes the loaded-field
  positions that locators address.

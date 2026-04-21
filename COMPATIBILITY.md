# Valkey Search Compatibility

## Goal

The Valkey Search module aims to enable applications currently built against the Redisearch module to run against Valkey Search with little or no application-side change. Concretely, a developer who has written an application using standard Redisearch client libraries and command syntax should be able to point that application at a Valkey deployment running the Valkey Search module and have it continue to function correctly.

Compatibility is framed from the perspective of the application developer. It is not a claim about internal equivalence between the two modules; it is a claim about the contract that applications depend on — the commands they send, the responses they parse, and the query semantics they rely on.

## Non-Goals

The following are explicitly **not** goals of Valkey Search compatibility with Redisearch. Differences in these areas are expected and are not considered bugs.

**Binary, on-disk, and replication-stream format compatibility.** The serialized representation of an index — including RDB payloads, AOF records, any internal persistence format, and the byte-level representation used on the replication stream between primary and replica nodes — is an implementation detail of Valkey Search. It is not expected to match Redisearch's format, and no tooling is provided to read or write one format from the other. In particular, a Valkey Search primary cannot replicate index state to a Redisearch replica, nor the reverse; cross-module replication topologies are not supported.

**Internal and source-level parity.** Valkey Search does not attempt to match Redisearch's internal APIs, module hooks, source structure, threading model, or internal data structures. Code that depends on Redisearch internals — rather than on its public command surface — is out of scope.

**Performance characteristics.** Latency, memory footprint, throughput, and scaling behavior are driven by Valkey Search's own implementation choices and may differ from Redisearch in either direction. Applications should not assume performance parity, and tuning guidance developed against Redisearch may not transfer.

**Error message text parity.** The exact wording of error strings returned to clients is not guaranteed to match Redisearch. The _semantic_ error conditions — that is, which operations fail and under what circumstances — are expected to align where command and query compatibility applies, but the human-readable text of error responses may differ.

## Expected Compatibility

This section describes the areas where Valkey Search intends to behave the same as Redisearch from an application's point of view. **Observable differences in these areas are considered bugs.** If an application written against Redisearch produces different results, fails where it previously succeeded, or succeeds where it previously failed in any of the following dimensions, that is a defect in Valkey Search and should be reported as such.

The contract below applies only to features that Valkey Search actually implements. Valkey Search does not include every feature of Redisearch; its supported feature set is a subset. An application that attempts to use a Redisearch feature Valkey Search does not support will receive an error — typically indicating that the command, option, field type, or query construct is not recognized or not implemented. That error is the intended behavior and is **not** a compatibility bug. The authoritative source for which features are supported is the Valkey Search documentation and the release notes for the specific release of Valkey Search being deployed; those should be consulted to determine the supported surface.

### Command and argument syntax

The set of commands exposed by the module, their argument ordering, their flag and option names, and the shape of their replies are expected to match Redisearch. This includes index management commands (e.g. `FT.CREATE`, `FT.DROPINDEX`), query commands (`FT.SEARCH`, `FT.AGGREGATE`), cursor commands, and other command-surface entry points that applications invoke directly. Applications using standard Redisearch client libraries should be able to issue these commands unmodified and receive replies they can parse with existing response handlers.

### Query language and semantics

The query language accepted by `FT.SEARCH` and `FT.AGGREGATE` — including text queries, tag filters, numeric filters, boolean operators, field modifiers, scoring behavior, return-value shapes, and vector search syntax — is expected to match Redisearch. A query that returns a given set of documents under a defined sort order against Redisearch should return the same documents in the same order against Valkey Search for an equivalently configured index.

Note, however, that some queries are inherently unsorted: when no sort order is specified (explicitly via `SORTBY` or implicitly via a score-producing query), Redisearch and Valkey Search are both free to return matching documents in any order, and that order may differ between the two modules. Equivalence in those cases means the same _set_ of documents, not the same sequence. Differences in result ordering for queries without a defined sort order are not considered compatibility bugs.

### Index types and data model

The supported field types (`TEXT`, `TAG`, `NUMERIC`, `VECTOR`, and others in the Redisearch surface), the storage models they apply to (hash-backed indexes and JSON-backed indexes), and the schema semantics that govern how documents are indexed and matched are expected to match Redisearch. Schema definitions authored for Redisearch should produce indexes with equivalent behavior when applied to Valkey Search.

### A note on stricter input validation

In some cases Redisearch silently accepts extraneous or malformed syntax — for example, unrecognized trailing arguments or query fragments that do not conform to the documented grammar — and returns a successful reply. Valkey Search may be stricter: in these cases it may reject the input with an error rather than accept it. This stricter validation is an intentional choice and is **not** considered a compatibility bug, even though it falls on a surface that is otherwise expected to be compatible. Applications that have been relying (knowingly or not) on Redisearch's tolerant parsing will need to clean up the offending call sites as part of migration.

### A note on attribute ordering within returned keys

Within a single document returned by `FT.SEARCH` or `FT.AGGREGATE`, the order in which attributes (field name / value pairs) appear is not guaranteed to match between Redisearch and Valkey Search. The _set_ of attributes returned for each document is expected to match; the sequence in which those attributes are laid out inside the reply may differ. This is **not** considered a compatibility bug.

### A note on floating-point precision

Floating-point arithmetic is inherently imprecise, and the observable result of a floating-point computation depends on the order in which individual operations are performed. Valkey Search and Redisearch do not guarantee that floating-point operations — including arithmetic involved in scoring, vector distance calculations, aggregations, numeric range comparisons, and the conversions performed when parsing floating-point values from text or formatting them back into text — are executed in the same order. Small numerical differences between the two modules on otherwise-equivalent inputs should be expected, and are **not** considered compatibility bugs. Applications that compare floating-point results for exact equality, or that depend on the exact textual representation of a floating-point value round-tripped through the module, should be reworked to tolerate small numerical differences.

## Intentional Incompatibilities

This section describes areas where Valkey Search intentionally diverges from Redisearch. Divergences here are by design and will not be treated as defects. Where possible, each item is described in terms of _what differs_, _why_, and _what migration impact to expect_.

### Configuration

**What differs.** The module's configuration surface — including configuration parameter names, how they are set, and how they are queried — does not match Redisearch's. In particular, configurables are exposed through Valkey's native configuration machinery (`CONFIG GET` / `CONFIG SET`) rather than through `FT.CONFIG`.

**Why.** Valkey Search intentionally uses Valkey's native configuration machinery so that its configurables are managed the same way as the rest of the server's configurables, rather than through a module-specific, parallel surface. This is a design choice about where configuration lives, not a limitation of any underlying format.

**Migration impact.** Configuration files and any automation that sets module parameters (for example, via `MODULE LOADEX` arguments or `FT.CONFIG`-style commands) will need to be rewritten against the Valkey Search configuration surface, using `CONFIG GET` / `CONFIG SET` or the equivalent mechanism exposed by the managed service provider. Applications that only consume configuration indirectly — through the default behavior of the module — are not affected.

### INFO metrics (global `INFO` and per-index `FT.INFO`)

**What differs.** The metrics emitted by Valkey Search — both the global module-level metrics reported by `INFO` and the per-index metrics reported by `FT.INFO` — do not match Redisearch's metric set name-for-name.

**Why.** The `FT.INFO` output format is not fully extensible, and its rigidity is a major cause of incompatibility: the format does not cleanly accommodate the additional or differently-shaped metrics that Valkey Search needs to report on its own implementation, so matching it verbatim would foreclose Valkey Search's ability to report meaningfully on its own behavior. In addition, because Valkey Search's implementation differs from Redisearch's, some Redisearch metrics simply have no meaningful equivalent in Valkey Search (and, conversely, Valkey Search exposes metrics with no Redisearch counterpart).

**Migration impact.** Monitoring dashboards, alerting rules, and any operational tooling that scrapes Redisearch metric names will need to be re-pointed at the Valkey Search equivalents or adapted to the Valkey Search metric set. Applications that do not consume these metrics programmatically are not affected.

### Cluster and sharding behavior

**What differs.** Redisearch relies on a separate Sentinel component, along with specific command options, to carry out cluster-wide operations. Valkey Search has this functionality built in: no separate component and no special command-level options are required to obtain cluster-wide behavior. Valkey Search provides cluster-wide consistency for both index-mutation commands — `FT.CREATE` and `FT.DROPINDEX` — and query commands — `FT.INFO`, `FT.SEARCH`, and `FT.AGGREGATE`. Cluster-wide operations are retried as needed until whole-cluster consistency is obtained, and time out if that cannot be achieved within the operation's timeout window.

**Why.** Folding cluster coordination into the module itself removes a separate deployable component and the operational complexity that comes with it, and produces a deployment model that aligns with how Valkey's own cluster mode is operated. Making cluster-wide consistency the default behavior — rather than something the caller has to opt into with special options — means applications do not need to know whether they are talking to a clustered deployment or a single-node deployment to get correct results.

**Migration impact.** Applications no longer need to invoke a separate Sentinel component or add cluster-specific options to their commands; any such options should be removed as part of migration. Operational tooling that deployed, monitored, or scaled the Sentinel component will no longer apply. Callers should be prepared for the possibility that a cluster-wide operation may return a timeout error when cluster-wide consistency cannot be established within the operation's timeout, and should handle that case — for example by retrying at the application level or surfacing the failure to the user — rather than treating success as guaranteed.

### Persistence and on-disk index format

**What differs.** The persistence format used to store index state (across RDB, AOF, and any internal format) does not match Redisearch's.

**Why.** The on-disk index format is an internal implementation detail and is not part of the compatibility contract. Fixing it would constrain Valkey Search's ability to evolve its storage and recovery strategies independently of Redisearch.

**Migration impact.** Redisearch RDB files cannot be loaded directly by Valkey Search, and vice versa. Migrations between the two must rebuild indexes from the underlying keyspace (hashes or JSON documents), not by copying persisted index state.

### Log messages

**What differs.** The content, format, severity, and frequency of log messages emitted by Valkey Search do not match those emitted by Redisearch. Individual log strings, the events that trigger them, and the structured fields (if any) they carry should all be considered Valkey-Search-specific.

**Why.** Log output reflects the module's internal implementation, which diverges from Redisearch's. Attempting to reproduce Redisearch's log surface verbatim would constrain Valkey Search's ability to report on events and conditions meaningful to its own implementation, and would produce misleading messages for events that do not correspond to anything in Redisearch.

**Migration impact.** Log-scraping rules, alerting triggers keyed on specific log strings, log-shipping parsers, and runbooks that instruct operators to look for particular Redisearch log phrases all need to be reviewed and updated against Valkey Search's log output. Applications that do not consume logs programmatically are not affected.

### Additional intentional differences

_(TBD — reserved for additional intentional incompatibilities to be enumerated. Each item should follow the same pattern: what differs, why, migration impact.)_

## Extensions

Valkey Search may extend the syntax and semantics beyond what Redisearch provides. Where an extension is additive — for example, a new option on an existing command, a new field type, a new query operator, or a new command entirely — it is not considered an incompatibility: applications written against Redisearch do not exercise these surfaces and are unaffected. Extensions are, however, Valkey-Search-specific: applications that adopt them lose portability back to Redisearch.

Extensions are documented alongside the features they extend rather than centralized here. When an extension modifies the behavior of an existing Redisearch surface in a non-additive way, that change is tracked as an intentional incompatibility in the section above, not as an extension.

## Inter-Release Compatibility of Valkey Search

Separately from compatibility with Redisearch, Valkey Search makes its own commitments about how it evolves across its own releases. These commitments are expressed through a two-tier classification that applies to configuration variables and to INFO metrics (both global and per-index). Every such item is marked in the code as either **App** or **Dev**.

**App-classified items** are part of Valkey Search's public surface and are governed by SemVer. No breaking change to an App-classified configuration variable or INFO metric will be made outside a major release. Renames, removals, semantic changes, and changes to value format are all considered breaking. Applications, dashboards, alerting rules, and operational tooling may rely on App-classified items within a given major version.

**Dev-classified items** exist strictly for internal development and diagnostics. They are subject to change in any release — explicitly including patch releases — without notice and without a migration path. Dev items may be renamed, removed, or have their semantics altered at any time. **Users must not rely on Dev-classified items** in applications, dashboards, alerts, scripts, or any other context where stability matters.

The external Valkey Search documentation covers App-classified items, and only App-classified items. The relationship is intended to be biconditional: every App-classified item is expected to appear in the external documentation, and every item that appears in the external documentation is expected to be App-classified. Dev-classified items are not documented externally — their presence, names, semantics, and output format are intentionally left as implementation details. Either direction failing — an App-classified item missing from the documentation, or a Dev-classified item appearing in it — is a documentation bug and should be reported so it can be corrected.

The `FT._DEBUG` command is treated on the same footing as Dev-classified items: its subcommands, arguments, and output are subject to change in any release, including patch releases. It is provided for internal development and diagnostics only, and should not be used from applications or production tooling.

This classification applies only to Valkey Search's inter-release compatibility story. It is independent of Redisearch compatibility: an item can be App-classified (stable across Valkey Search releases) and still fall under an intentional incompatibility with Redisearch, or vice versa.

## Migration Guide

This section describes the steps an application team should work through when migrating an existing Redisearch-based application to Valkey Search. The steps are ordered so that earlier steps surface blocking issues before later steps require code changes.

### 1. Verify feature coverage against the documentation

Before making any code changes, review the Valkey Search documentation and confirm that every Redisearch feature the application depends on is currently supported. Walk through the commands the application issues, the field types and index options it uses, the query syntax it constructs, and any module-specific behavior it relies on. Anything the application uses that is not listed as supported should be resolved — either by waiting for support, replacing the dependency, or re-scoping the migration — before proceeding to later steps. This check is cheapest to do first because it determines whether migration is viable at all.

### 2. Remove usage of `FT.CONFIG`

Valkey Search does not expose module configuration through `FT.CONFIG`. Configurables are managed through Valkey's native configuration machinery: `CONFIG GET` and `CONFIG SET` for self-hosted deployments, or whatever equivalent mechanism is exposed by the managed service provider (for example, a cloud console, a provider-specific API, or parameter groups). Any application code or operational tooling that issues `FT.CONFIG GET` or `FT.CONFIG SET` needs to be replaced with the corresponding Valkey configuration call, and the relevant configuration parameter names need to be updated to the Valkey Search equivalents.

### 3. Update `FT.INFO` response parsing

The response format of `FT.INFO` in Valkey Search differs from Redisearch's format. Application code that parses `FT.INFO` output — whether to drive operational logic, emit metrics, or display information in a UI — needs to be updated to parse Valkey Search's `FT.INFO` response shape. Plan to retest any code path that consumes `FT.INFO`, since silent parse mismatches can produce misleading values rather than outright errors.

### 4. Remove unsupported `INFO` and `FT.INFO` fields

Some fields that Redisearch exposes through `INFO` (module-level, global) and `FT.INFO` (per-index) have no equivalent in Valkey Search, either because the underlying metric does not apply to Valkey Search's implementation or because the format has been restructured. Dashboards, alerts, scripts, and application code that reference such fields need to have those references removed or remapped to the closest Valkey Search equivalent. Items classified as Dev (see _Inter-Release Compatibility of Valkey Search_ above) should likewise not be carried over into production monitoring, regardless of whether a Redisearch counterpart exists.

### 5. Open the gRPC ports used by Valkey Search (cluster mode only)

This step applies only to deployments running with cluster mode enabled (CME). In CME, Valkey Search uses gRPC for inter-node communication that Redisearch does not require, which means the cluster must allow traffic on the gRPC ports Valkey Search is configured to use. Review network policies, security groups, firewall rules, and any service-mesh or ingress configuration governing the Valkey nodes, and ensure the relevant gRPC ports are reachable between the nodes that need to communicate. This step is operational rather than application-side, but it is easy to overlook during migration and will cause otherwise-correct clustered deployments to fail at runtime if missed. Deployments with cluster mode disabled do not require this step, regardless of whether they consist of a single node or a primary with one or more replicas.

### 6. Remove dependencies on error message contents and on log messages

Two related classes of dependency need to be audited out of the application and its surrounding tooling.

The first is any code path that inspects the textual content of error replies — for example, matching on specific substrings returned by a failed `FT.SEARCH`, branching on the wording of a schema validation error, or using regular expressions against error strings to decide how to react. Error message text is not part of the compatibility contract (see _Non-Goals_ above); the semantic error condition is expected to align, but the wording may not. Rework these paths to branch on the type of operation and on structured signals (command, return code, context) rather than on error string contents.

The second is any dependency on log messages — not only on specific log text, but on the _existence_ of particular log entries at all. Code or tooling that waits for a specific log line to appear, counts occurrences of a phrase, or treats the absence of a message as a signal is relying on a surface that Valkey Search does not preserve from Redisearch. A given Redisearch log event may be absent from Valkey Search entirely, emitted at a different severity, or emitted under different conditions. Remove these dependencies and replace them, where possible, with signals drawn from commands, metrics, or application-level observability.

### 7. Audit assumptions about result ordering for unsorted queries

Queries that do not specify a sort order — either explicitly through `SORTBY` or implicitly through a score-producing query — return their matching documents in an undefined order, and that order is not guaranteed to match between Redisearch and Valkey Search (see _Query language and semantics_ above). This matters most when an ordering-sensitive operation is applied on top of an unsorted result. The common case to watch for is a `LIMIT` clause — whether stated explicitly or applied implicitly by a default page size — used against a query that has no sort order: which rows fall inside the limit and which fall outside can differ between the two modules, silently changing what the application sees.

Audit the application's queries and identify any that (a) have no defined sort order and (b) have their results sliced, paginated, truncated, or otherwise treated as if the ordering were meaningful. For each such query, either add an explicit `SORTBY` to make the ordering deterministic, or adjust the application to tolerate the fact that the returned sequence is an arbitrary permutation of the matching set.

### 8. Review custom result-parsing code for attribute ordering

Within each document returned by `FT.SEARCH` or `FT.AGGREGATE`, the order in which the document's attributes (field name / value pairs) appear inside the reply is not guaranteed to match between Redisearch and Valkey Search (see _Query language and semantics_ above). Standard Redisearch client libraries parse these replies into name-keyed maps or structured records and are not affected. Custom or hand-rolled result-parsing code, however, may be reading fields by position — for example, "the value at index 3 is the `price` field" — and will break when the attributes come back in a different sequence. Review any such code and change it to look up attributes by name rather than by position.

## Known Compatibility Defects

_(Placeholder — under development.)_

This section will enumerate known defects in surfaces that are intended to be compatible with Redisearch under the Expected Compatibility contract above. Items listed here are bugs, not design choices, and are tracked for resolution rather than documented as permanent differences.

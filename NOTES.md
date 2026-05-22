# Inter-Release Compatibility of Valkey Search

Separately from compatibility with Redisearch, Valkey Search makes its own commitments about how it evolves across its own releases. These commitments are expressed through a two-tier classification that applies to configuration variables and to INFO metrics (both global and per-index). Every such item is marked in the code as either **App** or **Dev**.

**App-classified items** are part of Valkey Search's public surface and are governed by SemVer. No breaking change to an App-classified configuration variable or INFO metric will be made outside a major release. Renames, removals, semantic changes, and changes to value format are all considered breaking. Applications, dashboards, alerting rules, and operational tooling may rely on App-classified items within a given major version.

**Dev-classified items** exist strictly for internal development and diagnostics. They are subject to change in any release — explicitly including patch releases — without notice and without a migration path. Dev items may be renamed, removed, or have their semantics altered at any time. **Users must not rely on Dev-classified items** in applications, dashboards, alerts, scripts, or any other context where stability matters.

The external Valkey Search documentation covers App-classified items, and only App-classified items. The relationship is intended to be biconditional: every App-classified item is expected to appear in the external documentation, and every item that appears in the external documentation is expected to be App-classified. Dev-classified items are not documented externally — their presence, names, semantics, and output format are intentionally left as implementation details. Either direction failing — an App-classified item missing from the documentation, or a Dev-classified item appearing in it — is a documentation bug and should be reported so it can be corrected.

The `FT._DEBUG` command is treated on the same footing as Dev-classified items: its subcommands, arguments, and output are subject to change in any release, including patch releases. It is provided for internal development and diagnostics only, and should not be used from applications or production tooling.

This classification applies only to Valkey Search's inter-release compatibility story. It is independent of Redisearch compatibility: an item can be App-classified (stable across Valkey Search releases) and still fall under an intentional incompatibility with Redisearch, or vice versa.

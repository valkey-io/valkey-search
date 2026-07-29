# Config & Metrics Visibility: Proposal

## Problem

Current gating is inconsistent and impractical:

| What | Gate | Problem |
|------|------|---------|
| Dev configs | `debug-mode` (immutable after startup) | Useless for runtime tuning. Only works in tests. |
| Dev metrics | `info-developer-visible` config | Inconsistent with configs. No access control. |
| FT._DEBUG | ACL category `@admin @dangerous` | Correct pattern, but others don't follow it. |

`debug-mode` being immutable after startup means Dev configs can never be changed in production — even by operators. This eliminates the entire use case of runtime tuning knobs for debugging live issues.

## Proposal: 2 Tiers, Unified Gating via ACL

### Tiers

| Tier | Meaning |
|------|---------|
| **App** | Customer-visible. Default ACL allows access. |
| **Dev** | Internal/operator-only. Requires `@admin` ACL permission. |

### Gating Rules (Dev tier)

| Resource | Gate |
|----------|------|
| Dev configs (`CONFIG SET`) | `@admin` ACL category |
| Dev metrics (`FT.INFO`) | `@admin` ACL category |
| FT._DEBUG commands | `@admin` ACL category (already done) |

### What Changes

1. **Remove `debug-mode` config entirely.** Replace with ACL check on the command/config path.
2. **Remove `info-developer-visible` config.** Dev metrics visible to users with `@admin` ACL.
3. **Gate Dev configs by `@admin` ACL** in `OnSetConfig` validation (replace `IsDebugModeEnabled()` check).
4. **Gate Dev metrics by `@admin` ACL** in `FT.INFO` output filtering.
5. **Dev configs become mutable at runtime** — for any user with `@admin` permission.

### Why ACL `@admin`

- Consistent with Valkey core: `DEBUG`, `CONFIG`, `MODULE` all use ACL categories.
- Consistent with FT._DEBUG which already uses `@admin @dangerous`.
- No special startup flags. No extra configs.
- Operators grant `@admin` to their management user → full Dev access.
- Application users without `@admin` → no Dev access.
- Works with ElastiCache/MemoryDB managed ACLs out of the box.

### What We Don't Need

- No 3rd tier. App + Dev covers all cases.
- No `debug-mode` startup flag.
- No `info-developer-visible` runtime toggle.
- No local-client checks (not how Valkey ACL system works).
- No separate gating mechanisms for configs vs metrics vs commands.

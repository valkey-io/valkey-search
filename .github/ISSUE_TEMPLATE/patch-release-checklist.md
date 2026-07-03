---
name: Patch Version Release Checklist
about: Tracking checklist for cutting a patch release (e.g. 1.2.0 -> 1.2.1)
title: "Valkey Search <version> patch release checklist"
---

See [Releasing a new Valkey-Search version](https://github.com/valkey-io/valkey-search/wiki/Releasing-a-new-Valkey%E2%80%90Search-version) for the full procedure. Copy this checklist into the release tracking issue and work through it.

---

## Prerequisite
- [ ] 1. Make a list of all the commits (especially the ones with label `bug`) that were added to the main branch after last patch release for this branch (e.g. https://github.com/valkey-io/valkey-search/compare/1.2...main) and audit which changes need to be backported for this patch release. Run this by a valkey-search maintainer.
- [ ] 2. Make a list of all the open issues and open PRs that need to be closed and added to this patch (especially the ones with label `bug`, and which are part of the project board, e.g. https://github.com/orgs/valkey-io/projects/58/views/1) and communicate a code cutoff date in the OSS weekly meeting.
- [ ] 3. Whichever change needs to be backported, ask a maintainer to add those items to the respective release branch's project board and mark the status "To be backported" so that valkey-ci-agent can generate auto backport PRs. To understand how the backporting agent works see [`BACKPORTING.md`](https://github.com/valkey-io/valkey-search/blob/main/BACKPORTING.md). You will need to keep an eye on backport PRs opened by the agent, review them, and ask a maintainer to merge them (e.g. https://github.com/valkey-io/valkey-search/pull/1212). (For merging backport PRs you can ask the maintainer to `Set DCO to pass` to fix the failing DCO check, as the agent does not sign off. Also please don't squash and merge backport PRs.)
- [ ] 4. Once all changes are backported, verify we didn't introduce a breaking change in the patch release, e.g. new functionality, new info/configs, changes that affect memory profile, RDB changes, etc.

## Release Checklist (MAINTAINER ONLY)
- [ ] 1. Confirm all fixes for this patch are merged and backported onto the release branch (check the project board).
- [ ] 2. Move released PRs from "To be backported" to "Done" on the project board.
- [ ] 3. Submit a PR to the release branch updating `src/version.h` and the release notes / `00-RELEASENOTES`: (This can be done by a non-maintainer.)
   - [ ] set `kModuleVersion` to the new version (e.g. 1.2.0 -> 1.2.1)
   - [ ] set `MODULE_RELEASE_STAGE` to `ga`
   - [ ] if this patch adds a new metadata encoding (rare), add the corresponding `kReleaseXY` constant
   - [ ] confirm `kMinimumServerVersion` is still correct
   - [ ] (1.0 branch only) version is inline in `src/module_loader.cc` (`MODULE_VERSION`), not `version.h`
   - [ ] add patch release notes to `00-RELEASENOTES`
- [ ] 4. Confirm CI is green on the release branch (unit, ASAN/TSAN, integration, macOS, clang-tidy/format, spell check).
- [ ] 5. Cut the release at [github.com/valkey-io/valkey-search/releases/new](https://github.com/valkey-io/valkey-search/releases), based on the release branch (e.g. `1.2`), tagged with the version (e.g. `1.2.1`). Name the release after the version.
- [ ] 6. Verify `trigger-search-release.yml` fired and dispatched `search-release` to `valkey-bundle`.
- [ ] 7. Review and merge the resulting `valkey-bundle` PR (packaged extension update).
- [ ] 8. Verify the release artifacts / downloads are available.

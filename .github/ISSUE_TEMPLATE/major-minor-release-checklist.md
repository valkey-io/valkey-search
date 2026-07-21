---
name: Major/Minor Version Release Checklist
about: Tracking checklist for cutting a major or minor release (e.g. new 1.3.0)
title: "Valkey Search <version> major/minor release checklist"
---

See [Releasing a new Valkey-Search version](https://github.com/valkey-io/valkey-search/wiki/Releasing-a-new-Valkey%E2%80%90Search-version) for the full procedure. Copy this checklist into the release tracking issue and work through it.

---

## Prerequisite
- [ ] 1. Make sure all feature work to be added to the major/minor release is done and audit all changes included for this release. Run this by a valkey-search maintainer.
- [ ] 2. Make a list of all the open issues and open PRs that need to be closed and added before the release (part of the project board, e.g. https://github.com/orgs/valkey-io/projects/58/views/1) and communicate a code cutoff date in the OSS weekly meeting.

## Release Checklist (MAINTAINER ONLY)
- [ ] 1. Confirm all changes for this release are merged (check the project board).
- [ ] 2. Create the release branch, named major.minor (e.g. `1.3`), forked from `main` at feature freeze, and create a GitHub project board for the release branch.
- [ ] 3. Register the new branch for automated backporting for the future: add a "To be backported" status option, and PR the branch + project number into valkey-ci-agent's `repos.yml` (e.g. https://github.com/valkey-io/valkey-ci-agent/pull/10).
- [ ] 4. Submit a PR to the release branch updating `src/version.h` and the release notes / `00-RELEASENOTES`: (This can be done by a non-maintainer.)
   - [ ] set `kModuleVersion` to the new version (e.g. 1.2.0 -> 1.3.0)
   - [ ] set `MODULE_RELEASE_STAGE` to `rc1` (then `rc2`, ... for later RCs; `ga` for the final release)
   - [ ] if this release adds a new metadata encoding, add the corresponding `kReleaseXY` constant
   - [ ] confirm `kMinimumServerVersion` is still correct
   - [ ] add release notes to `00-RELEASENOTES`
   - [ ] tag maintainers / TSC members for review
- [ ] 5. Confirm CI is green on the release branch (unit, ASAN/TSAN, integration, macOS, clang-tidy/format, spell check).
- [ ] 6. Cut the release at [github.com/valkey-io/valkey-search/releases/new](https://github.com/valkey-io/valkey-search/releases), based on the release branch, tagged with the version (e.g. `1.3.0-rc1`, later `1.3.0`). Name the release after the version.
- [ ] 7. Verify `trigger-search-release.yml` fired and dispatched `search-release` to `valkey-bundle`.
- [ ] 8. Review and merge the resulting `valkey-bundle` PR (packaged extension update).
- [ ] 9. Verify the release artifacts / downloads are available.
- [ ] 10. For each further RC, repeat steps 4-9 advancing `MODULE_RELEASE_STAGE` (`rc2`, ...); for the final release set it to `ga`.
- [ ] 11. After GA, open the next development cycle on `main` (set `MODULE_RELEASE_STAGE` back to `dev`).

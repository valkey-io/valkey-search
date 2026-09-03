# Contributing to valkey-search

Welcome, and thank you for your interest in contributing!

## Get started

- **Questions?** Open a [GitHub Discussion](https://github.com/valkey-io/valkey-search/discussions) or join the [Valkey Slack](https://join.slack.com/t/valkey-oss-developer/shared_invite/zt-2nxs51chx-EB9hu9Qdch3GMfRcztTSkQ). The `#valkey-search-module` and `#search-module-development` channels are dedicated to user and developer oriented discussions, respectively.
- **Found a bug?** [Open a bug report](https://github.com/valkey-io/valkey-search/issues/new?template=bug_report.md&title=%5BBUG%5D).
- **Have a feature idea?** [Open a feature request](https://github.com/valkey-io/valkey-search/issues/new?template=feature_request.md&title=%5BFEATURE%5D).

## Developer Certificate of Origin (DCO)

All commits must include a `Signed-off-by` line. This is how you certify that you authored the contribution and have the right to submit it under the project's license.

Add it automatically with the `-s` flag:

```bash
git commit -s -m "your commit message"
```

A signed commit looks like:

```
Signed-off-by: Jane Smith <jane@example.com>
```

PRs with unsigned commits will be blocked by the DCO check.

## How to contribute

The right path depends on the scope of your change. The governance model behind these
paths is described in [GOVERNANCE.md](GOVERNANCE.md).

### Submit a pull request directly

You can skip the issue-and-discussion step and open a pull request directly for:

- **Bug fixes** with limited scope.
- **Compatibility improvements** with limited scope.
- **Small extensions to functionality** with limited scope. Any documentation changes
  required by the extension must be included in the same pull request.

A change is considered **limited scope** when _all_ of the following hold:

- It is **not a breaking change**. The one exception is a change made to improve
  compatibility of an existing feature — see [COMPATIBILITY.md](COMPATIBILITY.md) for what qualifies and how
  such changes are handled.
- It causes **no degradation in CPU performance**.
- It causes **no increase in memory consumption**.

### Discuss larger changes first

Changes with a larger scope — anything that does not meet the limited-scope criteria
above, and in particular breaking changes, performance- or memory-impacting changes, and
larger feature work — should **not** start as a pull request. Instead:

1. Open an issue describing exactly what you want to accomplish and why. Use cases are
   important for proposals to be accepted.
2. Discuss the proposal with the community, including at one of the regular project
   [meetings](https://zoom-lfx.platform.linuxfoundation.org/meetings/valkey?view=week), to build consensus on the approach before substantial implementation work
   begins.

This avoids spending time writing code for a change that may not be a conceptual fit.

### Develop large changes in a fork

The largest changes carry a real risk of needing a major revision partway through
development. To keep that risk out of mainline, develop large changes in a forked
repository and **do not** submit them for merge into mainline until the fork has matured
to the point where the risk of a major revision is minimal:

- **For large changes to existing data structures and functionality**, this means enough
  development has been done to ensure a minimal risk of regression in memory consumption
  or CPU usage as the work matures.
- **For large changes that add new functionality**, a similar bar applies: the new code
  should be mature enough that the odds of it requiring substantial rework are minimal.

As with any larger change, start by opening an issue and discussing the approach with the
community (see [Discuss larger changes first](#discuss-larger-changes-first)) before
beginning development in the fork. Once the fork clears the bar above, follow the
[pull request procedure](#pull-request-procedure) to bring it into mainline.

### Pull request procedure

Once you know which path applies, submit your change as follows:

1. Fork the repository and create a topic branch off `main`.
2. Make your changes and commit with a DCO sign-off (`git commit -s`).
3. Push your branch and open a pull request against `valkey-io/valkey-search:main`.
4. Address any CI failures and review feedback. All CI checks must pass before merging.

To link your PR to an existing issue, include `Fixes #<issue-number>` or `Resolves #<issue-number>` in the PR description.

## Finding issues to work on

- Issues labeled [**`good first issue`**](https://github.com/valkey-io/valkey-search/labels/good%20first%20issue) are explicitly flagged as good entry points for new contributors.
- Issues labeled [**`help wanted`**](https://github.com/valkey-io/valkey-search/labels/help%20wanted) are higher-priority items where extra community involvement is welcome.
- Before starting work on an issue, leave a comment expressing your intent — this avoids duplicate effort.

## Slash commands

Repository collaborators and maintainers can use the following commands in PR and issue
comments to help do some trivial tasks. Comments from other users (new or unknown
contributors) are ignored with a short reply, since collaborators are added by maintainers.

| Command                       | Where       | What it does                                                                                            |
| ----------------------------- | ----------- | ------------------------------------------------------------------------------------------------------- |
| `/label <names>`              | PR or issue | Applies one or more labels from the repo's defined label set. Rejects unknown labels; no-ops if already applied. |
| `/remove-label <names>`       | PR or issue | Removes one or more labels. No-ops for labels that are not currently applied.                           |
| `/reviewer <usernames>`       | PR only     | Requests a review from one or more GitHub users. Each user must be a repo collaborator.                 |
| `/remove-reviewer <usernames>`| PR only     | Withdraws a pending review request from one or more users.                                              |
| `/resolves <issue-number>`    | PR only     | Appends `Resolves #<N>` to the PR body, linking the issue and auto-closing it on merge.                 |
| `/rerun`                      | PR only     | Re-runs all failed or timed-out CI jobs for the PR's current head commit.                               |
| `/duplicates`                 | PR or issue | Finds likely duplicate issues or pull requests and reports them.                                        |
| `/assign-reviewers`           | PR only     | Runs automatic reviewer assignment on demand (see below). No-op if the PR is already assigned.          |

The label and reviewer commands accept a comma-separated list, so several items can be
handled in one comment. The command must be the first thing in the comment, and each
command goes in its own comment.

**Example usage:**

```text
/label bug, documentation
/remove-label good first issue
/reviewer allenss-amazon, BCathcart
/remove-reviewer yairgott
/resolves 925
/rerun
```

When part of a request cannot be carried out — an unknown label, a user who is not a
collaborator, an item that was already applied — the bot replies with a per-item breakdown
and still carries out the rest. A request that fully succeeds gets no reply; check the PR's
labels or reviewers to confirm.

### Automatic reviewer assignment

When a pull request is opened, or an existing draft is marked ready for review, a first-pass
reviewer and a maintainer are requested automatically from the pools in
[`.github/reviewer-pools.json`](.github/reviewer-pools.json). The bot leaves a comment
naming both.

Assignments are balanced across each pool, so review load stays evenly distributed no matter
who authors pull requests or how quickly people review. The author is never assigned to their
own pull request.

The first-pass reviewer does the detailed review and drives the feedback loop; the
maintainer then does the final review and merges. Use `/reviewer` and `/remove-reviewer` to
adjust the assignment, and edit `.github/reviewer-pools.json` to change the pools — every
entry in it needs collaborator access, since a review cannot be requested from a
non-collaborator.

Once a PR has been auto-assigned it is tagged with the `auto-assigned-reviewers` label, so
re-opening it, toggling draft/ready, or removing the requested reviewers will not reassign.
To request a fresh set of reviewers, **remove the `auto-assigned-reviewers` label and comment
`/assign-reviewers`** — the same selection runs again and picks the next reviewers.
`/assign-reviewers` also works on PRs opened before automatic assignment existed, so they can
be backfilled with a single comment.

## Code of conduct

This project follows the [Valkey community code of conduct](https://github.com/valkey-io/valkey/blob/unstable/CODE_OF_CONDUCT.md).

## License

See [LICENSE](LICENSE) for the project's licensing. By contributing, you agree your contributions will be licensed under the same terms.

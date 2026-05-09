## Automated Backport Workflow

Adds a GitHub Actions workflow that automatically creates backport PRs when a label is applied.

### How it works

1. Merge a PR to `main`
2. Add a label (e.g. `backport/1.0`) before or after the original PR's merge
3. Workflow cherry-picks the squashed commit onto the target branch
4. A new PR is created targeting that release branch
5. If conflicts exist, the PR is still created with conflict markers for manual resolution

### Backport Labels

Use the following labels to automate the backport to the respective branch:
- `backport/1.0`
- `backport/1.1`
- `backport/1.2`

### Details

- Triggers on: PR merged + `backport/<branch>` label present
- Commits use `--signoff` for DCO compliance
- Backport PRs are assigned to the original author
- No auto-merge — all backport PRs require manual review even if no conflicts arise.
- No third-party services / apps — runs entirely on GitHub Actions with `GITHUB_TOKEN`

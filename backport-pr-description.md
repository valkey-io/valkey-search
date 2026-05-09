## Automated Backport Workflow

Adds a GitHub Actions workflow that automatically creates backport PRs when a label is applied.

### How it works

1. Merge a PR to `main`
2. Add a label like `backport/1.0` (before or after merge)
3. Workflow cherry-picks the squashed commit onto the target branch
4. A new PR is created targeting that release branch
5. If conflicts exist, the PR is still created with conflict markers for manual resolution

### Labels needed

Create these labels in the repo:
- `backport/1.0`
- `backport/1.1`
- `backport/1.2`

### Details

- Triggers on: PR merged + `backport/<branch>` label present
- Commits use `--signoff` for DCO compliance
- Backport PRs are assigned to the original author
- No auto-merge — all backport PRs require manual review
- No third-party services — runs entirely on GitHub Actions with `GITHUB_TOKEN`

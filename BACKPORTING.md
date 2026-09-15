# Backporting (Maintainer Guide)

This document explains how automated backporting works for valkey-search and how maintainers manage it.

## Overview

Backports to release branches are automated by [valkey-ci-agent](https://github.com/valkey-io/valkey-ci-agent). The agent runs a daily sweep (09:00 UTC), cherry-picks eligible PRs onto release branches, validates the build, and opens backport PRs for review.

## How it works

1. A PR is merged to `main`.
2. A maintainer adds the PR to the relevant GitHub Project board and sets its status to **"To be backported"**.
3. The agent discovers it on the next sweep, cherry-picks it onto the target branch, runs `./build.sh` to validate, and opens a backport PR.
4. If the cherry-pick has merge conflicts, the agent attempts AI-powered resolution and labels the PR with `ai-resolved-conflicts`.

## Project boards

Each release branch has a corresponding GitHub Project board in the `valkey-io` org:

| Branch | Project # | Link |
|--------|-----------|------|
| `1.0` | 36 | https://github.com/orgs/valkey-io/projects/36 |
| `1.1` | 34 | https://github.com/orgs/valkey-io/projects/34 |
| `1.2` | 58 | https://github.com/orgs/valkey-io/projects/58 |

Each board must have a **"Status"** field with a **"To be backported"** option. This is the value the agent queries via GraphQL to discover candidates.

## Requesting a backport (for maintainers)

1. Open the relevant project board above.
2. Add the merged PR to the board.
3. Set the PR's **Status** to **"To be backported"**.
4. The agent picks it up on the next daily sweep.

For an immediate on-demand backport (instead of waiting for the daily sweep), the `manual-backport.yml` workflow can be triggered. Currently this requires write access to the `valkey-ci-agent` repo. A mechanism to allow individual projects to trigger manual backports directly is planned.

```bash
gh workflow run manual-backport.yml \
  --repo valkey-io/valkey-ci-agent \
  --field pr_url=https://github.com/valkey-io/valkey-search/pull/<PR_NUMBER> \
  --field target_branch=<BRANCH>
```

## For contributors

Contributors do not have access to project boards or the manual workflow. If your PR fixes a bug that should be backported, mention it in your PR description — e.g., "This should be backported to 1.0 and 1.1." A maintainer will handle the rest.

## Adding a new release branch

When a new release branch is created (e.g., `1.3`):

1. Create a new GitHub Project board in the `valkey-io` org (e.g., "Valkey-search 1.3").
2. Add a **"To be backported"** option to the board's **Status** field.
3. Submit a PR to [valkey-ci-agent's `repos.yml`](https://github.com/valkey-io/valkey-ci-agent/blob/main/repos.yml) adding the new branch and project number under `valkey-io/valkey-search`:

```yaml
      - branch: "1.3"
        project_number: <new-project-board-number>
```

## Required labels

The following labels must exist in this repository for the agent to function:

| Label | Purpose |
|-------|---------|
| `backport` | Applied by the agent to backport PRs it creates |
| `ai-resolved-conflicts` | Applied when the agent used AI to resolve merge conflicts |

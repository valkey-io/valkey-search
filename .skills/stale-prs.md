# Stale PRs

Find open PRs in valkey-io/valkey-search that haven't been updated in the last 30 days.

## Steps

1. Use `gh` to list open PRs in `valkey-io/valkey-search` that were last updated more than 30 days ago:

```bash
gh pr list --repo valkey-io/valkey-search --state open --json number,title,author,updatedAt,url --limit 1000 | jq '[.[] | select(.updatedAt < (now - 2592000 | todate))] | sort_by(.updatedAt) | .[] | "PR #\(.number) - \(.title)\n  Author: \(.author.login)\n  Last updated: \(.updatedAt)\n  URL: \(.url)\n"' -r
```

2. Present the results as a table showing:
   - PR number and title
   - Author
   - Days since last update
   - URL

3. If no stale PRs are found, report that all open PRs have recent activity.

4. Offer to look into any specific stale PR in more detail (review changes, check if it's blocked, etc.).

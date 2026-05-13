# Duplicate Issues

Find potential duplicate issues in valkey-io/valkey-search by comparing a given issue against all open issues.

## Inputs

- **Issue number** (required): The issue to check for duplicates

## Steps

1. Fetch the target issue details:

```bash
gh issue view <number> --repo valkey-io/valkey-search --json title,body,labels
```

2. Fetch all open issues:

```bash
gh issue list --repo valkey-io/valkey-search --state open --json number,title,body,labels --limit 200
```

3. Compare the target issue against all open issues looking for:
   - Similar titles (keyword overlap, semantic similarity)
   - Similar descriptions (shared error messages, stack traces, code references)
   - Same labels or affected components
   - References to the same files, functions, or commands

4. Present results ranked by likelihood of being a duplicate:
   - **High confidence**: Very similar title AND description
   - **Medium confidence**: Similar title OR significant description overlap
   - **Low confidence**: Same component/label with some keyword overlap

5. For each potential duplicate, show:
   - Issue number, title, and URL
   - Confidence level
   - What specifically matched (shared keywords, similar error, same component)

6. If no duplicates are found, report that the issue appears unique.

7. Offer to check closed issues as well if no open duplicates are found.

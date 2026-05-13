# Backport

Cherry-pick a change from main onto one or more release branches and push backport branches to the user's fork.

## Inputs

- **Source** (required): A PR number or commit SHA from main
- **Target branches** (required): One or more existing upstream branches (e.g. `1.0`, `1.1`, `1.2`)

## Steps

### 1. Resolve the source commit

If given a PR number:
```bash
gh pr view <number> --repo valkey-io/valkey-search --json mergeCommit,title,number,body
```
Extract the merge commit SHA. If the PR is not yet merged, abort with a message.

If given a commit SHA, use it directly and fetch the commit message:
```bash
git log --format="%H %s" -1 <sha>
```

### 2. Determine fork remote

```bash
git remote -v
```

Look for the user's fork (origin). If not found, ask for the fork URL.

### 3. Validate target branches exist in upstream

```bash
git fetch upstream
git branch -r | grep "upstream/<branch>"
```

If a target branch does not exist, flag it and skip.

### 4. For each target branch, create a backport

```bash
git fetch upstream
git checkout -b backport-<branch>-<pr_number_or_short_sha> upstream/<branch>
git cherry-pick <commit_sha> --signoff
```

**Preserving original sign-off:** The `--signoff` flag adds the backporter's sign-off. The original author's `Signed-off-by` line is already in the commit message from the cherry-pick and will be preserved automatically.

### 5. Handle merge conflicts

If `git cherry-pick` fails with conflicts:

1. Note which files have conflicts:
   ```bash
   git diff --name-only --diff-filter=U
   ```

2. Stage the conflicted files as-is (with conflict markers) and commit:
   ```bash
   git add .
   git commit -C CHERRY_PICK_HEAD --signoff
   ```
   This first commit intentionally includes the conflict markers so reviewers can see what conflicted.

3. Flag the conflicts to the user clearly:
   - List each conflicted file
   - Show the conflict markers
   - Offer to resolve them in a follow-up commit on the same branch

4. Resolve the conflicts in a second commit so the reviewer can see the resolution as a separate diff.

### 6. Push to fork

```bash
git push origin backport-<branch>-<pr_number_or_short_sha>
```

### 7. Report results

For each target branch, report:
- Branch name pushed to fork
- Whether cherry-pick was clean or had conflicts
- If conflicts: list affected files and offer to fix them

### 8. Wait for user instruction to raise PRs

**Do NOT create PRs automatically.** Report the pushed branches and wait for the user to decide whether to open PRs.

If the user asks to raise PRs, create them with:
- **Title:** `Backporting <original PR title> (#<number>) to <branch>`
- **Base:** the target release branch
- **Body:**
  ```text
  <original PR description or summary>

  ---------

  Backporting https://github.com/valkey-io/valkey-search/pull/<number> to <branch>
  ```

## Naming Conventions

- Branch: `backport-<target_branch>-<pr_number>` (e.g. `backport-1.0-1007`)
- PR title: `Backporting <original title> (#<number>) to <branch>`

## Important Notes

- Always preserve the original author's `Signed-off-by` from the cherry-picked commit
- Add the backporter's sign-off via `--signoff` flag
- Never force-push or modify upstream branches
- If the same backport branch already exists on the fork, ask before overwriting

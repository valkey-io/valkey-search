---
name: write-integration-tests
description: Write or change Python integration tests under integration/. Use when adding a test file or test case, or when a new or edited integration test is flaky.
---

# Write Integration Tests

Start from the template in `integration/README.md` under "Adding new tests".

## Wait for backfill when the test depends on it

After `FT.CREATE`, backfill runs on the server cron and re-reads every key in the live keyspace. That includes keys the test writes after `FT.CREATE`, until the first cron tick finishes the scan.

Create the index with `wait_for_backfill=True` (or call `wait_for_backfill_complete`) before the test:

- needs a key that existed before `FT.CREATE`, written earlier or loaded from an RDB;
- mutates or deletes a key (HSET over, HDEL, DEL, RENAME, field or key expiry) and then asserts on index or registry state. The scan reads the key as it is at the tick, so it can undo the test's setup.

Pass `wait_for_backfill=False` only when the test checks behavior while backfill is still running. A test that only inserts keys and then queries is safe either way.

Example: `test_hdel_then_rename_out_of_prefix_does_not_crash` set an 80 ms field expiry right after `FT.CREATE`. When the first backfill tick landed after the expiry, the scan dropped the vector record and the test failed intermittently.

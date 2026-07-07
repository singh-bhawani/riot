# Inspiration behind the RIOT replicate safety flags

This note records *why* the following `riot replicate` flags exist — the real
production scenarios that motivated them — so the intent isn't lost over time.

Both are opt-in and leave default replication behavior unchanged.

---

## `--no-replace` — copy a key only if it is **not** already on the target

**Where it came from:** the OGS use case.

We had two things happening against the same target at once:

- an **old backup** acting as the replication *source*, and
- a **new-data write job** actively writing the latest data straight to the
  *target*.

The target already held the newer, authoritative data (from the live write
job). The old backup was only there to **backfill keys that were still missing**
on the target. Those missing keys would *eventually* be produced by the write
job too (after some hours), but we wanted them populated immediately — **without
clobbering the newer values already present on the target**.

So the idea was a mode that **copies a key from source to target only if that
key is not already present on the target**:

- key **absent** on target → write it (backfill from the old backup),
- key **present** on target → skip it (the live write job's newer value wins).

That is exactly what `--no-replace` does.

---

## `--ignore-expired` — don't propagate source **expirations** to the target

**Where it came from:** long-running RIOT replications, and specifically an
issue hit during a **search (index) migration**.

In a long-running sync, keys naturally **expire (TTL)** on the source. By
default RIOT treats a key that has vanished from the source as a deletion and
issues a `DEL` on the target. That is wrong when the target is authoritative:

- If the **TTL was carried forward** to the target during replication, the
  **target's own expiration** will remove the key when it lapses there. RIOT
  does **not** need to re-delete it — the target handles its own expiry.
- Propagating the source expiry as an explicit `DEL` is not only redundant, it
  actively caused problems — we hit an issue with this during the **search
  migration**, where these expiry-driven deletes on the target were harmful.

So `--ignore-expired` stops **expirations** on the source from being propagated:

- key **expired** on source (keyspace event `expired`) → **do not** delete on
  target (let the target's own TTL handle it),
- key **deleted** on source (`del` / `unlink`) → **still propagate** the delete
  (genuine deletions must replicate).

Only expirations are suppressed; real deletions continue to flow through.

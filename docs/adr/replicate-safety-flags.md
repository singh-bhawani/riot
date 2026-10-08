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

---

## `--source-mcache-id` / `--target-mcache-id` — move keys between memcoil mcacheIds

**Where it came from:** migrating data between mcaches via Meesho's memcoil
client.

When `keyPrefixEnabled` is on, memcoil stores every key as a **2-byte
big-endian binary mcacheId** followed by the original key bytes
(`addPrefixToKey` → `util.IntTo2ByteArray`), e.g. mcacheId `82` →
`[0x00, 0x52] + "user:1001"`, mcacheId `195` → `[0x00, 0xC3] + "user:1001"`.
Moving an mcache's data to a new mcacheId therefore means rewriting that
binary prefix — something the text `--key-prefix` (`--mcache`) cannot express.

The two options are independent and can be used alone or together:

- `--source-mcache-id <id>` → **strip** the prefix for `<id>` from source keys.
  Keys that do not start with exactly that prefix (or consist only of the
  prefix) are **skipped** and logged at debug level — so a shared source only
  yields the keys belonging to that mcache.
- `--target-mcache-id <id>` → **prepend** the prefix for `<id>` to keys.
- both (e.g. `--source-mcache-id 82 --target-mcache-id 195`) → **rewrite**
  `82`-prefixed keys to `195`-prefixed keys.

Notes:

- Ids must be in `0..65535`. Values are never modified.
- Runs first in the processor chain (before `--mcache`, `--ignore-expired`,
  `--no-replace`), so `--no-replace` checks existence of the *rewritten* key.
- `--target-mcache-id` cannot be combined with `--mcache` (both would prefix
  target keys); `--source-mcache-id` with `--mcache` is allowed.
- Live-mode delete events pass through the same rewrite, so a `DEL` of `82:key`
  on the source deletes `195:key` on the target.
- Dataset verification (compare) is disabled, since source and target keys
  intentionally differ.

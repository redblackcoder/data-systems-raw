# Apache Iceberg, End to End — An Engineer's Field Guide

*A long-form, fully-offline technical read. No links, no internet required — everything you need to reason
about Iceberg from first principles is on this page. Pairs with `iceberg-online-lab-and-resources.md` for
the hands-on lab and the clickable resources. Written against Iceberg **1.11.0** (format versions **v2**
and **v3**). Estimated ~18 pages.*

**Table of contents**
1. Why an open table format had to exist
2. The format, precisely (the metadata tree)
3. Reads and writes as an append-only log
   - 3a. How engines actually write: distributed write, central commit
4. Consistency guarantees (ACID on object storage)
5. Schema and partition evolution
6. Copy-on-Write vs Merge-on-Read
7. Advanced features for performance (stats, bloom filters, secondary indexes, Z-order, geospatial)
8. The library toolbox (how you actually work with Iceberg)
9. Catalogs — Glue and REST
10. Open metadata exchange across warehouses (Snowflake, Databricks, Data Cloud, BigQuery, Redshift)
11. The payoff and the honest caveats
- Appendix A: cheat sheet · Appendix B: interview Q&A · Appendix C: glossary · Appendix D: a real table, dissected

---

## 1. Why an open table format had to exist

For a decade the default "table" on a data lake was a **Hive table**: a directory in HDFS or S3, plus a
Hive Metastore entry saying "this table lives at `s3://…/sales/` and is partitioned by `dt`." A partition
was literally a sub-directory (`dt=2026-08-25/`), and the set of files in a table was *whatever happened to
be in those directories right now*. That design has four fatal flaws at scale:

**① No atomicity — there is no "commit," so readers see half-written tables.**
Picture a nightly job appending a day's data straight into the live partition directory:
```
s3://lake/sales/dt=2026-08-25/
    part-00000.parquet     ← writer streams files in, one at a time …
    part-00001.parquet
    …
    part-00299.parquet     ← writer crashes here (300 of 500 written)
```
A `SELECT count(*)` running at this instant happily reads the 300 files that landed and returns a
confidently wrong answer — nothing marks the table as "mid-write." The Hive-era fix is to write into a
staging directory and *rename* it into place at the end, because on **HDFS** a directory rename is a single
atomic namenode operation. But **S3 has no rename**: the committer must **COPY each object to its final key,
then DELETE the staging key**. That is O(number of files), slow, and — crucially — *not atomic*: mid-loop a
reader sees a partially-populated partition, and if the job dies at object 300/500 the table is permanently
half-committed, with no marker that anything is wrong and nothing to roll back. (This fragility is the whole
reason the Hadoop **S3A "directory" and "magic" committers** exist.)
→ *Iceberg's answer:* write all 500 files first, invisible to everyone; then flip them in with a **single
atomic pointer swap** at the catalog. A reader sees either the old snapshot or the new one — never 300 files.

**② Correctness depends on directory listing — and a directory isn't a transactional table version.**
To plan `SELECT * FROM sales WHERE dt = '2026-08-25'`, the engine must enumerate the files by listing the
prefix:
```
LIST s3://lake/sales/dt=2026-08-25/   → part-00000.parquet, part-00001.parquet, …
```
Three problems. **(1) Slow & paginated:** S3 `LIST` returns ≤1000 keys per call, so a partition of 100k
small files costs ~100 sequential round-trips *just to plan the query*, and a wide table multiplies that
across every partition. **(2) Consistency:** historically S3 `LIST` was only *eventually* consistent, so
immediately after a write a plan could **miss just-written files** (undercount) or still list files a GC job
had already **deleted** (→ `FileNotFoundException` when the reader opens them). S3 became strongly consistent
in late 2020, but a *table format* can't assume that of every object store — and strong consistency still
doesn't answer the real question. **(3) The deep problem:** "whatever objects are under this prefix *right
now*" is not a well-defined table state — a reader racing a writer or a compactor sees a different file set
than one a second later.
→ *Iceberg's answer:* the exact file list for a pinned snapshot is read straight from the **manifest** — no
`LIST`, fully deterministic, and identical for every reader of that snapshot.

**③ Schema is positional / matched-by-name — so evolution silently corrupts old files.**
Say the table is `users (id BIGINT, name STRING, email STRING)` and millions of old Parquet files were
written with those three columns in that order. With **position-based** resolution (classic Hive/Parquet),
dropping the middle column is a silent disaster:
```sql
ALTER TABLE users DROP COLUMN name;        -- table schema is now (id, email)
-- but old files still physically hold [id, name, email] at ordinals 0,1,2
-- reader binds ordinal 1 → the "email" column → every row's NAME is served as its EMAIL
```
No error is raised; queries just return wrong data. **Name-based** resolution has the mirror-image trap:
```sql
ALTER TABLE users RENAME COLUMN email TO email_address;
-- old files still contain a column literally named "email"
-- reader looks for "email_address", finds none → returns NULL for every historical row
```
That's silent data loss on the renamed column. Adding a column *in the middle* is equally unsafe under
positional matching — which is why plain Hive largely forbade it.
→ *Iceberg's answer:* every column gets a stable integer **field-id**; Parquet files carry those ids, so a
rename is pure metadata and add-in-the-middle / drop / reorder never rebinds bytes. A file missing a
field-id reads `null` (or a v3 default) — never another column's data.

**④ Partitioning leaks into queries — and re-partitioning breaks everything.**
"Partitioned by day" physically means directories keyed on a **separate `dt` string column**, distinct from
the real event timestamp:
```
s3://lake/events/dt=2026-08-25/…      -- dt is its own column, not the same as event_time
```
Users must *know* `dt` exists and filter on its exact form to get any pruning:
```sql
SELECT * FROM events WHERE dt = '2026-08-25';                     -- ✅ prunes to one directory
SELECT * FROM events WHERE event_time >= TIMESTAMP '2026-08-25';  -- ❌ Hive can't prune → full scan
```
So every pipeline hand-derives and stores `dt = CAST(event_time AS DATE)` and hopes the two never drift.
Worse, **changing the scheme** (daily → hourly) means physically rewriting the *entire* table into new
`dt=…/hr=…/` directories **and** editing every query and job that hard-coded `WHERE dt = …` — and you can't
keep old data in the old scheme and new data in the new one under a single table.
→ *Iceberg's answer:* **hidden partitioning** — declare `PARTITIONED BY day(event_time)` and a filter on
`event_time` *itself* prunes automatically; users never see or derive `dt`. **Partition evolution** then
lets you switch to `hour(event_time)` for new data with a metadata-only change — old data untouched, queries
unchanged.

Apache Iceberg (created at Netflix by Ryan Blue & Dan Weeks, donated to the ASF) reframes the problem. A
table is not a directory; it is an **immutable tree of metadata** whose root is tracked by a **catalog**.
The design cleanly separates three concerns that Hive tangled together:

```
     ENGINE            TABLE FORMAT              STORAGE
 (Spark, Trino,   ⟷   (Apache Iceberg:     ⟷   (S3, GCS, ADLS,
  Flink, Snowflake,     the spec + metadata       HDFS — just bytes)
  BigQuery, …)          tree + catalog SPI)
```

Because the *format* is an open specification — not a product — any engine can implement a reader/writer,
and the same physical files are one source of truth for all of them. That is the whole thesis, and every
feature below is downstream of it: **atomic commits, time travel, schema/partition evolution, hidden
partitioning, and multi-engine interoperability all fall out of "a table is an immutable, versioned tree
of metadata over immutable data files, with a catalog holding the current root pointer."**

---

## 2. The format, precisely (the metadata tree)

Everything in an Iceberg table except the current-root pointer lives as **immutable files in object
storage**. Writers only ever *add* files; they never mutate an existing metadata or data file in place.
This immutability is what makes snapshots, time travel, and lock-free concurrency possible.

```
CATALOG  ──(the one mutable pointer)──▶  metadata.json         (a.k.a. "table metadata file")
                                          • table UUID, format-version (1|2|3), table location
                                          • schemas[]           + current-schema-id
                                          • partition-specs[]   + default-spec-id
                                          • sort-orders[]       + default-sort-order-id
                                          • properties{}        (table config)
                                          • snapshots[]         each ─▶ ONE manifest list
                                          • current-snapshot-id, refs{} (branches & tags),
                                            snapshot-log[], metadata-log[]
                                                     │
                          manifest list (Avro,       │  one per snapshot
                          one file per snapshot)  ◀──┘
                                          • entry per manifest: path, added/existing/deleted counts,
                                            partition-spec-id, and PARTITION FIELD SUMMARIES
                                            (per-partition-column lower/upper bounds)   ◀── prune whole manifests
                                                     │
                          manifest file (Avro)   ◀───┘  lists data & delete files
                                          • per file: path, format, partition tuple, record_count,
                                            column_sizes, value_counts, null_value_counts,
                                            lower_bounds{}, upper_bounds{}                ◀── prune individual files
                                          • content: 0 = DATA, 1 = POSITION_DELETES, 2 = EQUALITY_DELETES
                                          • sequence_number (often inherited from the snapshot)
                                                     │
                          data files (Parquet / ORC / Avro)  +  delete files / deletion vectors
```

A few points that interviewers probe:

- **File formats.** Data files are usually **Parquet** (also ORC/Avro). The *metadata* files — manifest
  lists and manifests — are **Avro** (row-oriented, cheap to append and scan). `metadata.json` is JSON.
  Table-level statistics are stored in **Puffin** files (see §7).
- **Two levels of pruning, before any data file is opened.** Query planning walks
  `metadata.json → manifest list → manifests`, discarding whole manifests by their partition summaries,
  then discarding individual data files by their per-column `lower_bounds`/`upper_bounds`/null counts.
  **Reads never `LIST` the object store** — the file set is read *from metadata*, which also kills the
  eventual-consistency problem.
- **Field IDs, not names.** Every column has a stable integer **field-id**. Parquet columns are matched to
  the schema by field-id, so rename/reorder are metadata-only and never corrupt old files (see §5).
- **Snapshots.** Each write produces a new immutable **snapshot** with a `snapshot-id`, a parent id, the
  `manifest-list` location, a `summary` (operation = `append`/`overwrite`/`delete`/`replace` + metrics),
  and a **sequence number**. Time travel = "read the table as of snapshot X (or timestamp T)."
- **Sequence numbers** are a monotonically increasing logical clock assigned at commit. They are the key
  to merge-on-read correctness (§6): a delete file with sequence number *N* applies only to data files
  with sequence number ≤ *N*.
- **Refs: branches and tags.** `refs{}` maps names to snapshots. `main` is the default branch. You can
  create an audit branch, write to it, validate, then fast-forward `main` — the **Write-Audit-Publish**
  pattern. Tags (e.g., `end-of-day-2026-08-25`) pin a snapshot for retention/reproducibility.
- **The logs.** `snapshot-log[]` records the (timestamp → snapshot-id) history used for
  time-travel-by-timestamp; `metadata-log[]` tracks previous `metadata.json` files.

A useful one-sentence mental model: *a table is a stack of immutable snapshots; a snapshot is a set of
manifests; a manifest is an annotated list of data/delete files; and the catalog just remembers which
`metadata.json` is current.*

---

## 3. Reads and writes as an append-only log

**A write, step by step (say an `INSERT`/append):**
1. The engine writes new **data files** (Parquet) to the table's data location.
2. It writes one or more new **manifest files** listing those data files with their column statistics.
3. It writes a new **manifest list** for the new snapshot (reusing unchanged manifests from the parent
   snapshot by reference — this is why commits are cheap even for large tables).
4. It writes a new **`metadata.json`** that appends the new snapshot and moves `current-snapshot-id`.
5. It asks the **catalog** to atomically swap the root pointer from the old `metadata.json` to the new one
   (§4). If that swap fails because someone else committed first, the whole thing retries.

Note what did *not* happen: no existing file was modified, and no directory was listed. The "mutation" is
purely the pointer swap in step 5.

**A read, step by step:**
1. Ask the catalog for the current `metadata.json` (or a specific snapshot for time travel).
2. Resolve the snapshot → its manifest list.
3. **Prune manifests** using partition-field summaries against the query predicate.
4. **Prune data files** within surviving manifests using per-column min/max/null stats.
5. Open only the surviving data files; apply any applicable **delete files / deletion vectors** (§6) for
   merge-on-read; evaluate residual predicates.

This is why Iceberg planning scales: pruning is metadata arithmetic, and the amount of metadata read is
proportional to the *relevant* slice of the table, not its total size.

---

## 3a. How engines actually write: distributed write, central commit

§3 described a write abstractly ("the engine writes files, then commits"). In a distributed engine, *the
engine* is really **many parallel workers plus one coordinator**, and how they split the labor is the
single most useful thing to know about Iceberg writes. Spark, Trino, and Flink all follow the same
two-phase pattern — and all embed the **`iceberg-core` Java library** for the commit half, so the
manifest-writing code is literally the same across them.

**Phase 1 — parallel write (workers/executors).** Each task writes **data files** (Parquet) — and, for
merge-on-read updates/deletes, **delete files** — directly to object storage, then returns lightweight
**`DataFile` / `DeleteFile` descriptors** (path, partition tuple, record count, column stats). **Workers do
not write manifests.**

**Phase 2 — central commit (one node: Spark driver / Trino coordinator).** It gathers every descriptor from
every task, calls the Iceberg API (`AppendFiles` / `OverwriteFiles` / `RowDelta`), which writes the
**manifest file(s)**, the **manifest list**, and the new **`metadata.json`**, then performs the single
**atomic catalog swap** — the operation §4 is about.

```
workers  ──▶  part-*.parquet  (+ delete files)   ──DataFile descriptors──▶   driver / coordinator
 (N tasks → N+ data files, no manifests)                                      │
                                                                              ├─ writes manifest(s)   (Avro)
                                                                              ├─ writes manifest list (Avro)
                                                                              ├─ writes metadata.json  (JSON)
                                                                              └─ ATOMIC catalog CAS  ← the commit
```

So a **500-task append writes ~500 Parquet files but typically ONE new data manifest** (a few if grouped by
partition spec, or if Iceberg auto-merges small manifests) + one manifest list + one metadata.json + one
commit. **Manifest count tracks the commit, never the worker count** — the commit must be a single atomic
act at the catalog; you can't have 500 tasks committing independently.

⚠️ "One Parquet file per task" isn't exact either. A task rolls over to a new file at
`write.target-file-size-bytes` (~512 MiB default), and in a *partitioned* write a task writes one file per
partition it touches. To stop that exploding into tiny files, `write.distribution-mode` (commonly `hash`
for partitioned tables) shuffles rows so each task owns few partitions; `fanout-enabled` lets a task keep
several partition writers open without pre-sorting the input.

**Spark internals.** The final write stage runs executor tasks using Iceberg's `TaskWriter`/`FileAppender`
(behind the DataSource-v2 `DataWriter`); each task returns a `WriterCommitMessage` carrying its `DataFile`s.
`SparkWrite.commit(messages)` on the **driver** aggregates them and runs the Iceberg commit.

**Trino internals.** On workers, the `TableWriterOperator` drives the connector's **`IcebergPageSink`**,
which writes the Parquet (and MOR delete) files and emits **`CommitTaskData`** fragments. On the
**coordinator**, the `TableFinishOperator` collects the fragments and `IcebergMetadata.finishInsert()` /
`finishMerge()` runs the same iceberg-core commit. For partitioned writes, Trino inserts a partitioning
exchange (`use_preferred_write_partitioning`) — the analogue of Spark's `hash` distribution.

### What each operation writes (append vs update vs delete)

Update/delete fork on **copy-on-write (COW)** vs **merge-on-read (MOR)**, chosen per operation via
`write.update.mode` / `write.merge.mode` / `write.delete.mode`. Both engines support both.

| Operation | Workers write | Commit phase writes | Iceberg API → snapshot op |
|---|---|---|---|
| **INSERT / append** | new **data files** | data manifest(s) + list + metadata.json | `AppendFiles` → `append` |
| **UPDATE/DELETE — COW** | **rewritten data files** | new data manifest(s); old files' refs removed | `OverwriteFiles` → `overwrite` |
| **UPDATE — MOR** | **delete files** + new **data files** | separate **delete manifest** + **data manifest** | `RowDelta` → `overwrite` |
| **DELETE — MOR** | **delete files** only (positional) | **delete manifest** | `RowDelta` → `delete` |

Nuances that matter (and make good interview points):
- **COW** rewrites whole data files; the commit *adds new files and drops references to old ones* — a
  metadata-level file removal, **not** a delete file. Read-fast, write-amplifying.
- **MOR batch deletes from Spark/Trino are positional** (`(file, pos)`); **equality deletes** are mainly a
  **Flink streaming/upsert** artifact, not batch Spark/Trino.
- **Metadata-only delete:** when the predicate aligns with whole partitions/files (e.g.
  `DELETE … WHERE dt='2020-01-01'` on a `dt`-partitioned table), **no files are written at all** — the
  commit just removes those data files' references. The cheapest delete; both engines optimize it.
- **Data and delete files live in separate manifests** (`content: 0=data, 1=pos-del, 2=eq-del`) — which is
  why an MOR update emits two manifests.
- **v3:** positional deletes become **deletion vectors** — one Puffin bitmap per data file instead of many
  small positional-delete files.
- **Compaction is a write too:** `rewrite_data_files` reads small files + applies deletes on workers, writes
  large data files, and commits via `RewriteFiles` (`replace` op) — the maintenance counterpart that folds
  MOR deletes back into data.

**Takeaway:** workers produce data (and delete) files in parallel and report their metadata; a single
coordinator turns that metadata into manifests + a manifest list + `metadata.json` and commits with one
atomic swap. Everything §4 says about isolation and OCC applies to *that swap*, not to the parallel file
writes — which is why the writes need no locking and a failed task just leaves orphan files to GC (§4, §7.7).

---

## 4. Consistency guarantees (ACID on object storage)

This is the crux, and a favorite interview area. Object stores give you durable bytes and — critically —
**atomic single-object writes**, but no multi-object transactions and (historically) weak listing
consistency. Iceberg builds full ACID table semantics on top of that thin substrate.

**Atomicity.** A commit is atomic because it reduces to a **single pointer swap** at the catalog: either
the new `metadata.json` becomes current, or nothing changes. All the data/manifest files written before
the swap are invisible until the swap succeeds (nothing references them yet), and are just garbage to be
cleaned up later if the commit is abandoned.

**Isolation.**
- **Readers get snapshot isolation.** A reader pins a snapshot id at planning time and sees a consistent,
  immutable view for the life of the scan — concurrent writers adding snapshots cannot perturb it. This is
  effectively **serializable reads** against the pinned snapshot.
- **Writers get optimistic concurrency control (OCC).** There are no locks on the hot path. Instead a
  commit carries two things (this is the same structure the REST catalog exposes, §9):
  - **requirements** — assertions about the *current* table state that must still hold, e.g.
    `assert-ref-snapshot-id(main = 123)`, `assert-table-uuid`, `assert-current-schema-id`,
    `assert-last-assigned-field-id`.
  - **updates** — the ordered metadata changes to apply (add-snapshot, set-snapshot-ref, add-schema, …).
  The commit algorithm: **validate every requirement against the current metadata; if all hold, apply the
  updates, write the new `metadata.json`, and atomically swap the pointer.** If the pointer moved
  underneath (someone else committed), the swap fails and the writer **re-reads the new base, re-validates,
  and retries** with bounded backoff.

**Serializability vs snapshot isolation for writes.** Iceberg supports both isolation *levels* for
write operations, chosen per operation via table properties (`write.<op>.isolation-level`):
- **serializable** (default for many ops) — the commit validates that *no conflicting data files were
  added* by concurrent commits in the affected partitions/predicate range. Prevents write-skew.
- **snapshot isolation** — a weaker, higher-throughput level that only checks the ref hasn't moved in a
  way that invalidates the specific files being replaced. Allows more concurrency at the cost of some
  serializability guarantees.
The engine performs **conflict detection at commit** by scanning the manifests added since the read
snapshot and checking them against the operation's conflict predicate (e.g., "did anyone else touch
partition `dt=2026-08-25`?"). If a real conflict is found, the commit fails and must be re-planned.

**Durability** is delegated to the object store (11 nines on S3, etc.); Iceberg never claims data is
committed until the metadata that references it is durably the current root.

**What Iceberg does *not* give you for free:** cross-table transactions. A single commit is atomic for
**one table**. Atomic multi-table commits require a catalog that supports them — the REST spec has a
`/transactions/commit` endpoint, and Nessie offers git-style multi-table commits — but the base table
format guarantees are per-table. This is a common "gotcha" question.

**The linchpin is the catalog's atomic swap.** The whole ACID story rests on the catalog being able to do
a **compare-and-set** on the root pointer: "set current = new *only if* current is still old." Different
catalogs implement that CAS differently — a Hive lock, a Glue table-version check, a JDBC transaction, a
**DynamoDB conditional write**, a Nessie commit — but the *contract* is identical. That contract is exactly
what §9's `TableOperations.commit(base, metadata)` encodes.

### How a commit is actually validated (metadata-only, and conservative)

The conflict check runs **entirely on manifest metadata — it never re-reads row data**, and only a *writer*
runs it (a pure reader has no commit; it just keeps reading its pinned snapshot's files). When writer A
(base = S0) finds the table has advanced to S1, Iceberg walks the snapshots committed in between and, from
their manifests, enumerates the data/delete files they **added** and **removed** — each manifest entry
carries status ADDED/EXISTING/DELETED, the file's **partition tuple**, and per-column **lower/upper bounds +
null counts**.

- **Snapshot isolation** asks only: *did a concurrent commit remove or replace a file I depend on?* A
  operates on a specific set of files (say `f1`); SI checks they're still live. If S1 merely **added** `f2`
  and reused `f1`'s manifest unchanged, `f1` is intact → SI passes. Stated precisely, the rule is *"the
  files my operation touches weren't concurrently replaced,"* not *"no manifest I read changed."*
- **Serializable** additionally asks: *could any concurrently-added data file possibly contain a row
  matching my filter?* A carries a **conflict-detection filter** (an Iceberg `Expression` from its
  `WHERE`/`ON`). For each newly-added data file it tests, **metadata-only**: (a) the file's **partition
  tuple** vs the partition predicate (exact), and (b) the file's **column bounds/nulls** vs the residual
  predicate (could `user_id=42` fall within `[min,max]`?). It is the *same pruning math as read planning,
  run in reverse*: cannot-match → ignore; might-match → **conflict → fail → re-plan**. So it's
  **conservative** — it may raise a *false* conflict and force a needless retry, but it never misses a real
  one and never opens a data file.

"But my predicate is arbitrarily complex." Iceberg evaluates only the parts resolvable against partition
values + column bounds (comparisons, ranges, `IN`, `AND`/`OR`, transform projections). Anything it can't
(a UDF, `lower(name) LIKE …`, cross-column arithmetic) is treated as **possibly-matching** — safe, just
more false conflicts. It never does exact row-level evaluation, because that would mean reading the data.
(Both levels use the two-level metadata structure to stay cheap: manifest-list partition summaries skip
whole manifests before any entry is examined.)

### Immutability is the substrate

**Data files are immutable — once a Parquet file is written it is never modified in place.** Every change
is *additive*: write **new** files (data files, and/or delete files / deletion vectors) in a **new
snapshot**, and mark the old files *removed* in the new manifest set. The old objects still exist (older
snapshots reference them → time travel) until `expire_snapshots` + `remove_orphan_files` GC what no live
snapshot needs. The **only** in-place mutation in the whole system is the catalog pointer swap.

This is exactly why readers get snapshot isolation *for free*, and it resolves an apparent paradox ("how is
a table consistent while it's being modified?"):
- A **MOR delete** does **not** touch `f1`; it writes a **new delete file** (positional entries / a v3
  deletion vector for `f1`) in a **new snapshot**. A reader pinned on S0 never sees that snapshot, so its
  view — `f1` plus the deletes that existed as of S0 — cannot change underneath it.
- A **COW delete/update** reads `f1` and writes a *new* file `f1' = f1` minus/with the changed rows; the
  commit marks `f1` removed and `f1'` added. `f1` itself is untouched. SI holds *as long as `f1` wasn't
  concurrently replaced* — if B had rewritten `f1`, the data `f1'` derived from changed underneath A, so the
  overwrite is rejected and re-planned.
Both reduce to *"the files I read were not concurrently replaced,"* which is only a cheap, meaningful check
**because files never mutate — they are only added or retired.**

### What serializable still can't catch (the honest limit)

Iceberg validates the files you **write** against a filter; it does **not** track the rows you **read** to
decide that write (no SSI-style read/write-dependency tracking). So genuine **write-skew across disjoint
files** slips through even at `serializable`:

> Invariant (app-enforced): region `west` must keep **≥1** engineer on call — currently Alice and Bob.
> **T1** reads west (2 ≥ 1 ✓) → `UPDATE oncall SET on_call=false WHERE name='Alice'`; **T2** reads west
> (2 ≥ 1 ✓) → `UPDATE … WHERE name='Bob'`. Alice and Bob are in **different files**: T1's conflict filter
> `name='Alice'` doesn't match T2's `name='Bob'` file, so serializable finds no conflict. **Both commit →
> 0 on call.** No serial order does that (serially the second reads 1 and aborts). Iceberg can't see it
> because the anomaly lives in the **reads**, not in overlapping **written** files.

The same gap covers **cross-table invariants** (T1 writes A after reading B while T2 writes B after reading
A): single-table validation can't see the other table — you'd need a multi-table transaction (§9's
`/transactions/commit`, or Nessie). Rule of thumb: **`serializable` prevents phantoms *inside the predicate
range you write*; it is not database-grade SERIALIZABLE over arbitrary read sets.**

---

## 5. Schema and partition evolution

**Schema evolution** is safe because of field-ids. Allowed, metadata-only operations:
- **add** a column (old files simply lack it → read as `null`, or as a v3 default value);
- **drop** a column (its field-id is retired; old files' bytes are ignored);
- **rename** (name is metadata; field-id is the identity);
- **reorder**;
- **widen** a type along safe promotions (`int→long`, `float→double`, `decimal(p,s)→decimal(p',s)` with
  larger precision).

Because matching is by field-id, none of these ever rewrite or corrupt existing data files. Narrowing
types or incompatible changes are disallowed.

**Hidden partitioning** removes the classic Hive footgun. You declare a **partition spec** as a set of
**transforms** over columns; Iceberg computes and stores partition values in metadata. Users never see a
synthetic partition column and never need a special predicate — a filter on the *source* column
(`WHERE event_ts >= '…'`) is automatically translated to partition pruning. Transforms:

| Transform | Meaning | Typical use |
|---|---|---|
| `identity` | value as-is | low-cardinality dims |
| `bucket[N]` | hash into N buckets | high-cardinality keys, even distribution |
| `truncate[W]` | truncate string/number to width W | prefix grouping |
| `year`/`month`/`day`/`hour` | date/time bucketing | time-series |
| `void` | always null | drop a field from partitioning |

**Partition evolution.** You can change the partition spec *without rewriting existing data*. Each spec
gets an id; old data files keep their old spec, new data files use the new one, and planning handles the
mix. So a table that was `day(ts)` can become `hour(ts)` for new data, and nothing has to be rebuilt. Sort
orders evolve the same way (`sort-orders[]` + `default-sort-order-id`), which matters for the clustering
techniques in §7.

### What's in a data file vs. in metadata (and why old files keep their spec)

A point that resolves a common confusion: **the data file does not contain a partition column.** A Parquet
data file holds the **raw source columns** (`event_ts`, `user_id`, …). The **partition value** —
`day(event_ts)=2026-08-25`, `bucket[16](user_id)=7` — is *computed by the transform at write time* and
stored as the file's **partition tuple in its manifest entry**, tagged with the **partition-spec-id** that
produced it. (Directory paths like `event_ts_day=2026-08-25/` are a physical convenience; planning trusts
the manifest tuple, not the path.) Writers route rows so that **every row in one data file shares a single
partition tuple** under the spec in force — which is why one tuple per file is accurate.

That makes both "stores values in metadata" and "old files keep their old spec" true simultaneously:
- **You can't re-tag an old file under a new spec by rewriting the manifest.** A file written as one `day`
  partition may hold rows spanning 24 `hour` values — there is no single valid `hour` tuple for it. Honestly
  re-tagging would require **reading the rows and re-splitting the file**, i.e., rewriting data. So Iceberg
  doesn't.
- **Partition evolution is therefore metadata-only:** append the new spec to `partition-specs[]` and move
  `default-spec-id`. Old files keep their original spec-id and tuple (valid only for the spec that computed
  it); new writes use the new spec. `partition-specs[]` retains every spec ever used.
- **Planning handles the mix** by *projecting the query predicate through each spec* — files under
  `month(ts)` pruned by the month projection, files under `day(ts)` by the day projection, in one scan
  (each manifest/file knows its spec-id). The source columns being physically present is also what makes
  residual filtering — and an optional later rewrite — possible.
- To actually migrate old data into the new layout, run **`rewrite_data_files`** (compaction): it reads
  rows and rewrites files under the new spec. Optional and deferrable forever — *"nothing has to be
  rebuilt"* means *nothing is forced*, not *it can't be done*.

---

## 6. Copy-on-Write vs Merge-on-Read

Row-level mutations (`UPDATE`/`DELETE`/`MERGE INTO`) are where the format version and the write strategy
matter most.

**Copy-on-Write (COW).** To change any row in a data file, the engine **rewrites the entire file** with the
change applied and swaps it into the new snapshot. Reads are maximally fast (just read data files, no merge
step), but writes amplify — changing one row in a 512 MB file rewrites 512 MB. Good for batch pipelines and
read-heavy tables.

**Merge-on-Read (MOR).** Instead of rewriting data files, the engine writes small **delete files** that
mark rows as removed; readers subtract them at scan time. Writes are cheap and fast (good for streaming and
frequent small mutations), at the cost of read-time merge work and small-file accumulation.

Two kinds of delete file in **format v2**:
- **Positional deletes** — rows of `(data_file_path, row_position)`. "In *this* file, positions 5, 12, 900
  are deleted." Produced by `UPDATE`/`DELETE`/`MERGE` when the engine knows which rows to kill.
- **Equality deletes** — a predicate like `(id = 42)` or `(user_id = X AND day = Y)`. "Delete any row
  matching these column values." Produced mainly by **streaming upserts / CDC**, where the writer knows the
  key but not the physical position.

**Sequence numbers make this correct.** A delete file is stamped with the sequence number of its commit and
applies only to data files with a **smaller-or-equal** sequence number. So a delete committed at seq 10
removes matching rows from data written at seq ≤ 10, but *not* from data appended later at seq 11 — exactly
the semantics you want.

**Format v3 replaces positional-delete sprawl with deletion vectors.** A **deletion vector (DV)** is a
single compact binary bitmap (a **roaring bitmap**) per data file, stored in a **Puffin** file, encoding
"which row positions in this file are deleted." One DV per data file (rather than many small positional
delete files) means far fewer objects, faster reads, and simpler maintenance. v3 also adds **row lineage**
(`_row_id` + `_last_updated_sequence_number`) which makes efficient incremental/CDC reads and change
queries possible, plus new types (`variant`, `geometry`/`geography`, nanosecond timestamps) and column
**default values**.

**Choosing, in practice.** Set it per operation:
`write.update.mode`, `write.merge.mode`, `write.delete.mode` = `copy-on-write` | `merge-on-read`.
Rule of thumb: **batch + read-heavy → COW; streaming/CDC + write-heavy → MOR**, and then run compaction
(§7) on a cadence to fold deletes back into data files and control small files. The interview soundbite:
*"COW optimizes reads and pays at write; MOR optimizes writes and pays at read; sequence numbers keep MOR
correct; v3 deletion vectors make MOR cheap."*

---

## 7. Advanced features for performance

Iceberg's performance story is "read as little as possible, as sequentially as possible." The mechanisms
stack from cheap-and-always-on to advanced-and-opt-in.

### 7.1 Column statistics (always on, the workhorse)
Every manifest entry stores, **per column, per data file**: `record_count`, `value_counts`,
`null_value_counts`, `nan_value_counts`, and truncated **`lower_bounds`/`upper_bounds`** (min/max). This is
what powers file pruning: a predicate `amount > 1000` skips any file whose `upper_bound(amount) ≤ 1000`
without opening it. Two consequences interviewers like:
- **Sortedness multiplies the value of stats.** If data is clustered/sorted on the filtered column, min/max
  ranges per file are tight and non-overlapping, so pruning eliminates almost everything. Random data →
  overlapping ranges → little pruning. Hence sorting/Z-order below.
- **Stats are truncated** (bounds are stored to a bounded length) to keep manifests small; they're
  conservative (may over-include, never under-include).

### 7.2 Table-level statistics & Puffin (for the cost-based optimizer)
Beyond per-file min/max, Iceberg stores **table-level statistics** in **Puffin** files — most importantly
**NDV (number of distinct values)** estimated via **Theta sketches** (Apache DataSketches). A cost-based
optimizer (Trino, Spark) uses NDV and column sizes to order joins, choose broadcast vs shuffle, and
estimate selectivity. Puffin is the same container format v3 uses for deletion vectors.

### 7.3 Bloom filters (point-lookup acceleration)
For high-cardinality equality predicates (`WHERE user_id = 'abc'`), min/max ranges don't help much. Iceberg
writes **Parquet row-group bloom filters** when you enable them per column via write properties
(`write.parquet.bloom-filter-enabled.column.<col> = true`, with a configurable FPP). A reader consults the
bloom filter to skip row groups that certainly don't contain the value. Bloom filters live *inside* the
Parquet files (a file-format feature Iceberg drives), not in Iceberg metadata — a subtlety worth stating.

### 7.4 Secondary indexes (the current gap — and where engines/vendors extend)
Core Iceberg deliberately has **no general secondary-index structure** (no B-tree/inverted index in the
spec today). Its "indexing" is the pruning stack above: partitioning + sort order + min/max stats + bloom
filters + Puffin sketches. When teams need true secondary indexes (e.g., an inverted index for search, or a
per-value index for high-selectivity lookups on unsorted columns), they build them **outside the spec** —
as engine-specific index files, a sidecar index service, or catalog-tracked auxiliary structures. Good
interview framing: *"Iceberg gives you data-skipping, not secondary indexes; secondary indexing is an
active area layered on top."*

### 7.5 Data layout: Z-order and space-filling curves
File pruning is only as good as data clustering. Sorting by a single column makes that column prune well
but leaves others random. **Z-order** (and **Hilbert-curve**) sorting interleaves the bits of *multiple*
columns so that rows close in multi-dimensional space land in the same files — giving *good simultaneous
pruning on several columns* instead of great pruning on one. You apply it during **compaction**
(`rewrite_data_files` with a `sort_order` / `zorder(colA, colB)` strategy). This is the standard trick for
tables filtered on several dimensions (e.g., `region` + `event_type` + `ts`).

### 7.6 Geospatial
Geospatial filtering is inherently multi-dimensional (lat/long ranges, bounding boxes, nearest-neighbor),
so the same space-filling-curve idea applies: cluster points by a **Hilbert curve** over (lat, long) —
or over a geohash / S2 / H3 cell id — so that spatially-near rows are physically co-located and min/max
bounds prune by bounding box. Iceberg **v3 adds first-class `geometry` and `geography` types**, which lets
engines store proper spatial bounds and (over time) push down spatial predicates rather than treating
coordinates as opaque doubles. Until spatial predicate pushdown is universal, the pragmatic pattern is:
store a Hilbert/geohash cluster key, partition/sort on it, and let min/max + partition pruning do the work.

### 7.7 Maintenance (the operational half of performance)
Advanced features degrade without upkeep. The standard maintenance procedures (exposed as Spark `CALL`
procedures and in the engines):
- **`rewrite_data_files`** — compaction: bin-pack small files into large ones, apply pending deletes, and
  optionally re-sort / Z-order. The single most important table-health operation.
- **`rewrite_manifests`** — keep manifests well-sized and clustered by partition so pruning stays cheap.
- **`expire_snapshots`** — drop old snapshots and their now-unreferenced metadata; prerequisite for data
  cleanup and for bounding metadata growth.
- **`remove_orphan_files`** — delete files no snapshot references (e.g., from failed commits). Run
  carefully so it doesn't race in-flight writers.
- **deletion-vector / position-delete maintenance** — fold MOR deletes back into data files.

---

## 8. The library toolbox (how you actually work with Iceberg)

Iceberg is a spec plus a set of implementations. Knowing which library does what is practical interview
signal.

- **`iceberg-core` (Java)** — the reference implementation and the conceptual model everyone copies. Key
  abstractions:
  - **`Catalog`** (+ `SupportsNamespaces`) — name→table resolution, create/drop/rename, list.
  - **`TableOperations`** — per-table `current()` metadata and `commit(base, metadata)`; this is where the
    **atomic CAS** lives. Implementing a new catalog = implementing these two.
  - **`FileIO`** — pluggable storage (`S3FileIO`, `HadoopFileIO`, GCS, ADLS). Decouples the format from the
    filesystem; this is why "just bytes" storage works.
  - **`CatalogHandlers`** — turns any `Catalog` into the REST protocol's request/response semantics
    (validates requirements, applies updates). The shortcut for building a REST catalog (see the lab doc).
- **PyIceberg (Python)** — pure-Python client: read/write, catalog access (REST/Glue/SQL/Hive), integrates
  with PyArrow/DuckDB/pandas/Polars. Great for lightweight pipelines and for poking a catalog without a JVM.
- **iceberg-rust** — a growing Rust implementation (powers Lakekeeper and increasingly other tools);
  reference for a from-scratch, non-JVM reader/writer and catalog.
- **iceberg-go** — Go implementation, used in Go data tooling.
- **Engine connectors** — the read/write integrations that most users actually touch:
  - **Apache Spark** — the most complete: DataFrame + SQL, `MERGE INTO`, all maintenance procedures,
    branching. The de-facto reference engine.
  - **Apache Flink** — streaming ingestion (append + equality-delete upserts) and batch.
  - **Trino / Presto** — interactive SQL, CBO using Iceberg stats, read + write + maintenance.
  - **Apache Hive, Dremio, DuckDB, ClickHouse, Snowflake, BigQuery, Redshift** — varying degrees of
    read/write support (see §10).
- **File-format libraries** — **Parquet** (data), **Avro** (manifests/manifest lists), **Puffin**
  (statistics + deletion vectors), **Apache DataSketches** (Theta sketches for NDV).

The mental split: **`iceberg-core`/PyIceberg/iceberg-rust are the table-format brains; the engine
connectors are the muscle; FileIO is the storage adapter; the catalog is the coordinator.**

---

## 9. Catalogs — Glue and REST

A **catalog** does two jobs and only two: (1) resolve `namespace.table` to the current `metadata.json`
pointer, and (2) provide the **atomic compare-and-set** on that pointer for commits (§4). It is *not* where
data or rich metadata lives. Everything else a catalog offers (auth, RBAC, credential vending, discovery)
is value-add around those two primitives. We'll go deep on the two you asked for.

### 9.1 The AWS Glue Data Catalog
Glue is a managed, region-scoped metadata service (originally the Hive Metastore's cloud successor). As an
Iceberg catalog (`GlueCatalog` in `iceberg-aws`):
- Each Iceberg table is a **Glue table** whose parameters carry `metadata_location` (the current
  `metadata.json`) and `previous_metadata_location`.
- A commit is an **optimistic update** of that Glue table: read the current `metadata_location`, write the
  new `metadata.json` to S3, then `UpdateTable` **conditioned on the table version** so a concurrent commit
  causes a failure and a retry. That version check is Glue's CAS.
- **Strengths:** fully managed, integrated with the AWS analytics ecosystem (Athena, EMR, Redshift
  Spectrum, Lake Formation for fine-grained access control), no servers to run.
- **Limits (why big multi-tenant lakehouses outgrow it):** account/region quotas — on the order of
  **~100k tables**, **~20k partitions per table**, and throttled API throughput; single-region; multi-tenant
  isolation is coarse. When a lakehouse gives every tenant many tables and commits at high QPS, these
  ceilings bind.

### 9.2 The Iceberg REST Catalog
The REST catalog is not a storage backend — it's a **protocol**, an HTTP contract (defined by an OpenAPI
spec) that any catalog implementation can speak and any engine can call. This indirection is why it has
become the industry standard: **engines depend only on the REST API, and the server hides whatever is
behind it** (Glue, a JDBC DB, DynamoDB, Nessie, an in-house store). You can change the backing store
without touching a single engine — the migration superpower.

Core endpoints (base `/v1/{prefix}/…`, where `prefix` lets one server host many catalogs/warehouses):
- **`GET /v1/config`** — first call; returns `defaults`, `overrides`, and the list of supported
  `endpoints`. The client merges config (`defaults` < client < `overrides`) and learns capabilities.
- **Namespaces** — `GET/POST /namespaces`, `GET/HEAD/DELETE /namespaces/{ns}`,
  `POST /namespaces/{ns}/properties`.
- **Tables** — `GET /namespaces/{ns}/tables` (list), `POST …/tables` (create; `stage-create:true` for
  atomic CTAS), `GET …/tables/{t}` → **LoadTableResult** (metadata + optional `config` + optional vended
  `credentials`), `POST …/tables/{t}` → **commit**, `DELETE`/`HEAD`, plus `register`/`rename`, and advanced
  `credentials`, `sign` (S3 remote signing), `metrics`, and server-side scan planning (`plan`/`tasks`).
- **Views**, **functions**, and **`/transactions/commit`** (multi-table atomic).

**The commit is the heart** and mirrors §4 exactly. `POST …/tables/{t}` sends a `CommitTableRequest`:

```jsonc
{
  "requirements": [                              // assertions vs CURRENT server metadata
    {"type": "assert-table-uuid", "uuid": "…"},
    {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 123}
  ],
  "updates": [                                   // ordered MetadataUpdates to apply
    {"action": "add-snapshot", "snapshot": { … }},
    {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 456}
  ]
}
```

Server algorithm: load current metadata → check **every** requirement (fail ⇒ **409 CommitFailed**) → apply
updates in order → write the new `metadata.json` → **atomically swap** the pointer → return the new
metadata. Unknown requirement/update types must be **rejected (400)** — the forward-compatibility rule.
Two concurrent writers off the same base both build valid updates, but only one CAS wins; the loser gets
409 and retries against the new base.

Two REST features that matter operationally:
- **Credential vending** — `LoadTableResult` can return **scoped, short-lived storage credentials** (e.g.,
  STS tokens) so engines never hold long-lived cloud keys; the catalog becomes the central place to enforce
  per-table storage access. The alternative is **remote signing** (`/sign`), where the catalog signs each
  S3 request.
- **Multi-tenancy** — `prefix` + namespaces + per-request identity let one deployment isolate many tenants
  and scope storage centrally.

Well-known REST implementations to anchor the concept: **Apache Polaris** (from Snowflake), **Lakekeeper**
(Rust), **Unity Catalog** (Databricks, read-only Iceberg REST), **Nessie** (git-like), and Iceberg's own
`RESTCatalogServer` used in tests. (Other catalog types — Hive, JDBC, DynamoDB, Nessie — differ only in how
they implement the CAS; the REST protocol is the abstraction over all of them.)

---

## 10. Open metadata exchange across warehouses

Here is where the "open format" thesis pays its biggest dividend. Historically, getting data out of a
warehouse meant an **export/ETL** into another system — a second physical copy, a sync pipeline, drift, and
double storage. If every engine can instead read and write **the same Iceberg tables** through a **shared
catalog**, you get *one physical copy of the data, many engines*. The catalog — increasingly the **Iceberg
REST catalog** — is the interoperability contract. Below, how the major platforms plug in (state of the
world ~2025–2026; verify specifics with the vendor-doc links in the companion file).

### 10.1 Snowflake
- **Iceberg tables** come in two flavors: **Snowflake-managed** (Snowflake *is* the catalog, does the
  writes and lifecycle maintenance like compaction, and stores Parquet + Iceberg metadata in your
  **external volume** — customer-owned S3/GCS/Azure — or Snowflake-managed storage) and
  **externally-managed / catalog-integration** (an external catalog is the source of truth; Snowflake reads
  it).
- **Catalog integrations** it can consume: a **remote Iceberg REST catalog** (including **AWS Glue** and
  **Snowflake Open Catalog** — Snowflake's managed **Apache Polaris**), plus reading Iceberg/Delta metadata
  directly from object storage.
- **External engines reading Snowflake-managed tables** do so through Snowflake's **Horizon Catalog REST
  API** / an Iceberg catalog SDK, with **vended credentials** — i.e., Snowflake exposes its managed tables
  *as* an Iceberg REST catalog to the outside world.

### 10.2 Databricks
- **Delta UniForm (Universal Format)** asynchronously generates **Iceberg metadata alongside the Delta
  log**, over a **single copy** of the Parquet files (no rewrite). Iceberg clients can then read a Delta
  table as if it were an Iceberg table.
- **Unity Catalog acts as an Iceberg catalog**, exposing tables to external Iceberg engines via the
  **Iceberg REST Catalog API** (read access; a public-preview capability at time of writing). Delta Sharing
  recipients can likewise read Delta tables as Iceberg.
- **Caveats to know:** UniForm's Iceberg exposure is **read-only** for Iceberg clients; some Delta features
  (e.g., deletion vectors under IcebergCompatV2) aren't represented. Databricks' 2024 acquisition of
  **Tabular** (the company Ryan Blue founded) signaled deep Delta↔Iceberg convergence.

### 10.3 Salesforce Data Cloud
- Data Cloud stores customer data in an **open Iceberg lakehouse**.
- Interop shows up as **BYOL ("bring your own lake")** — federating over customer Iceberg/Delta data — and
  **zero-copy / data sharing** with Snowflake and Databricks, i.e., exchanging Iceberg tables/metadata
  rather than copying rows.

### 10.4 Google BigQuery
- **BigQuery tables for Apache Iceberg** are managed Iceberg tables where BigQuery does the writes and
  maintenance and stores Iceberg metadata in your **GCS**, so external Iceberg engines can read them.
- **BigLake** provides the metastore/catalog side: **BigLake metastore** can act as an Iceberg catalog, and
  **BigLake external tables** let BigQuery read Iceberg tables managed elsewhere. The pattern mirrors
  Snowflake's: managed-write + open-read, with a catalog exposed to other engines.

### 10.5 Amazon Redshift & the AWS lakehouse
- **Redshift** reads Iceberg tables through **Redshift Spectrum**, using the **AWS Glue Data Catalog** as
  the catalog — the classic "warehouse reads the lake" path.
- **Amazon S3 Tables** are AWS-managed Iceberg tables stored in a new **table bucket** type with a built-in
  table catalog and **automatic maintenance** (compaction, snapshot management, unreferenced-file removal).
  They **integrate with the Glue Data Catalog** for discovery, so **Athena, EMR/Spark, Redshift**, and
  SageMaker can all query the same tables. This is AWS packaging the open format as a managed service.

### 10.6 The pattern, and the failure modes
Notice the shape repeats everywhere: **managed write by the home engine + open read by everyone else,
brokered through a catalog that speaks (or is exposed as) the Iceberg REST API.** The REST catalog is
becoming the lingua franca precisely because it is the one contract all of these can agree on. But
interop has sharp edges worth naming in an interview:
- **Catalog divergence** — two engines pointed at *different* catalogs over the *same* files will disagree
  about the current snapshot. There must be one authoritative catalog per table.
- **Single-writer discipline** — most cross-engine setups are "one writer, many readers." Concurrent writes
  from different engines/catalogs risk lost commits unless they share the same catalog's CAS.
- **Format-version skew** — an engine that only reads v2 chokes on v3 deletion vectors / new types. Interop
  is bounded by the *minimum* format version all participants support.
- **Feature representation gaps** — UniForm not exposing deletion vectors, or a reader ignoring row lineage
  — mean "readable" isn't always "fully faithful."

---

## 11. The payoff and the honest caveats

**Why the open format is leverage:**
- **Vendor neutrality & no re-ingest.** One copy of the data in your own object store; swap or add engines
  (Spark for ETL, Trino for BI, Snowflake for business users) without copying or migrating.
- **Engine plurality.** Use the best tool per workload against the same tables.
- **Governance in one place.** A shared catalog centralizes access control, credential vending, and audit.
- **Operational reversibility.** Because the catalog is just a pointer store, you can migrate the *backing
  store* (Glue → DynamoDB → REST) without changing engines.

**The honest caveats (say these — they show maturity):**
- **You now run a lakehouse.** Compaction, snapshot expiry, orphan cleanup, and manifest rewriting are
  *your* job; neglecting them yields the small-files death spiral and ballooning metadata.
- **The catalog is a new critical path.** It's the concurrency arbiter and, with vending, the credential
  broker — a single point of failure and a security-sensitive service to run well.
- **Feature skew across engines is real.** v3, deletion vectors, row lineage, and geospatial land in
  different engines at different times; interop is bounded by the least-capable participant.
- **Metadata can itself become a scale problem.** Millions of tables/snapshots stress the catalog and the
  metadata tree — which is exactly why organizations build custom, low-latency catalogs.

**The one-paragraph takeaway.** Iceberg turns "a pile of Parquet files" into a real table by adding an
immutable, versioned metadata tree and a catalog that holds a single mutable root pointer. Atomic
commits, snapshot-isolated reads, schema/partition evolution, hidden partitioning, and time travel all
follow from immutability + an atomic pointer swap. Row-level change is COW (rewrite files) or MOR (delete
files / v3 deletion vectors), kept correct by sequence numbers. Performance comes from a pruning stack —
partitioning, sort/Z-order clustering, per-file min/max stats, bloom filters, and Puffin sketches — not
from secondary indexes, which remain a layer built on top. And because the whole thing is an open spec with
a REST-catalog contract, one physical copy of the data can be written by one engine and read by all the
others — which is why every major warehouse is converging on it.

---

## Appendix A — Cheat sheet

- **Metadata tree:** `catalog pointer → metadata.json → manifest list → manifest → data/delete files`.
- **Files:** data = Parquet/ORC/Avro; manifests & manifest lists = **Avro**; stats + deletion vectors =
  **Puffin**; `metadata.json` = JSON.
- **Snapshot** = one write; has a **sequence number**; refs = **branches/tags**; time travel by id/timestamp.
- **Commit** = **requirements (assert) + updates (apply) + atomic CAS**; conflict ⇒ **409** ⇒ re-plan/retry.
- **Isolation:** readers = snapshot isolation; writers = OCC with `serializable` (default) or
  `snapshot` levels; conflict detection at commit.
- **Evolution:** field-ids ⇒ safe add/drop/rename/reorder/widen; **hidden** + **evolvable** partitioning &
  sort orders.
- **Deletes:** positional `(file,pos)` · equality `col=value`; **v3 deletion vectors** = per-file roaring
  bitmap in Puffin.
- **COW** (rewrite files; fast reads) vs **MOR** (delete files; fast writes); set via
  `write.{update,merge,delete}.mode`.
- **Perf stack:** partition prune → manifest-summary prune → file min/max prune → bloom filters →
  Puffin NDV for CBO; cluster with **Z-order/Hilbert**; geospatial via v3 `geometry`/`geography` + curves.
- **Maintenance:** `rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`.
- **Catalog = pointer store + concurrency arbiter.** Glue CAS = table-version check; REST = protocol over
  any backend; DynamoDB = conditional write; Hive = lock; Nessie = commit.
- **Interop rule:** one authoritative catalog per table; interop bounded by the min format version.

## Appendix B — Interview Q&A (rapid fire)

1. *What makes an Iceberg commit atomic?* A single compare-and-set of the root `metadata.json` pointer at
   the catalog; everything else was pre-written and invisible until the swap.
2. *Reader isolation?* Snapshot isolation — a pinned, immutable snapshot for the whole scan.
3. *Two writers at once?* OCC: both build off a base; requirements are validated at commit; one CAS wins,
   the loser gets 409 and retries against the new base.
4. *Serializable vs snapshot isolation for writes?* Serializable also checks no conflicting files were
   added in the affected range (prevents write-skew); snapshot is weaker/faster.
5. *Cross-table transactions?* Not in the base format — needs a catalog that supports it (REST
   `/transactions/commit`, Nessie). Single-table commits are atomic.
6. *Positional vs equality deletes?* Position = `(file,pos)` (UPDATE/DELETE/MERGE); equality = `col=value`
   (streaming/CDC upserts). v3 → deletion vectors.
7. *How does sequence number keep MOR correct?* A delete at seq N applies only to data with seq ≤ N.
8. *COW vs MOR tradeoff?* COW = read-fast/write-amplify; MOR = write-fast/read-merge.
9. *How does pruning work without indexes?* partition summaries → per-file min/max/null → bloom filters →
   (for the optimizer) Puffin NDV. Sortedness makes min/max prune well.
10. *Why Z-order?* Cluster on multiple columns at once so several predicates prune, not just one.
11. *Does Iceberg have secondary indexes?* No general ones in the spec; data-skipping only. Secondary
    indexing is layered on top (engine/vendor extensions).
12. *What does a catalog minimally do?* Resolve `ns.table` → current metadata.json + atomic CAS on commit.
13. *Why REST catalog over Glue directly?* One protocol; engines decoupled from the store; central auth +
    credential vending + multi-tenancy; swap the backing store with no engine change.
14. *How do warehouses interoperate?* Managed-write by the home engine, open-read by others, brokered by a
    shared (increasingly REST) catalog over one physical copy.
15. *Biggest interop risk?* Two engines on different catalogs over the same files → snapshot disagreement /
    lost writes. One authoritative catalog per table.

## Appendix C — Glossary

- **metadata.json** — the table metadata file; the root the catalog points at.
- **manifest list** — per-snapshot Avro file listing manifests with partition summaries.
- **manifest** — Avro file listing data/delete files with per-column stats.
- **snapshot** — an immutable table version produced by one commit.
- **sequence number** — monotonic logical clock ordering data vs deletes.
- **ref** — a named pointer to a snapshot: **branch** (moves) or **tag** (pinned).
- **partition spec / transform** — the rule mapping column values to partitions (identity, bucket,
  truncate, year/month/day/hour, void).
- **COW / MOR** — copy-on-write (rewrite files) / merge-on-read (delete files).
- **deletion vector** — v3 per-file roaring-bitmap of deleted positions (in Puffin).
- **Puffin** — container for table statistics (Theta-sketch NDV) and deletion vectors.
- **FileIO** — Iceberg's pluggable storage abstraction.
- **TableOperations** — per-table `current()` + `commit(base, metadata)`; where the atomic CAS lives.
- **catalog** — resolves names to the current metadata pointer and provides the atomic CAS.
- **REST catalog** — the HTTP protocol (OpenAPI) that standardizes catalog access across engines.
- **credential vending** — catalog returns short-lived scoped storage credentials to engines.
- **UniForm** — Databricks feature emitting Iceberg metadata alongside Delta over one copy of data.
- **Polaris** — open-source Iceberg REST catalog (Snowflake origin); "Snowflake Open Catalog" is the
  managed form.

---

## Appendix D — A real table, dissected (generated with PyIceberg)

Everything in §2–§7, shown on an actual table. **All values below are verbatim output from Iceberg
0.11.1 (format-version 2)**, generated with PyIceberg against a local SQLite catalog + filesystem
warehouse. Re-run it yourself with the recipe in D.1.

### D.1 The table (reproducible recipe)
```python
# uv run --with 'pyiceberg[sql-sqlite]' --with pyiceberg-core --with pyarrow --with fastavro app.py
schema = Schema(
    NestedField(1, "event_ts",   TimestampType()),
    NestedField(2, "user_id",    LongType()),
    NestedField(3, "event_type", StringType()),
)
spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="event_ts_day"))
tbl = catalog.create_table("analytics.events", schema=schema, partition_spec=spec)
tbl.append(pa.table({                         # 3 rows spanning TWO days -> 2 partitions -> 2 data files
    "event_ts":   ["2026-08-25 10:00", "2026-08-25 23:30", "2026-08-26 01:15"],  # timestamp[us]
    "user_id":    [42, 7, 42],
    "event_type": ["click", "view", "click"],
}))
```

### D.2 Physical layout it produced
```
analytics/events/
  data/event_ts_day=2026-08-25/00000-0-e39f46ca-….parquet   (1384 B)   # 2 rows
  data/event_ts_day=2026-08-26/00000-1-e39f46ca-….parquet   (1362 B)   # 1 row
  metadata/00000-….metadata.json    (848 B)   # table at CREATE (empty)
  metadata/00001-….metadata.json    (1708 B)  # after APPEND (current — catalog points here)
  metadata/snap-8910707449840442321-0-….avro  # manifest LIST (one per snapshot)
  metadata/e39f46ca-…-m0.avro                  # manifest (lists the 2 data files)
```
The `event_ts_day=…` directory is a physical convenience; the authoritative partition value is in the
manifest (D.5).

### D.3 `metadata.json` (00001, trimmed + annotated)
```jsonc
{ "format-version": 2, "table-uuid": "551e7d4d-…",
  "schemas": [{ "schema-id": 0, "fields": [
      { "id": 1, "name": "event_ts",   "type": "timestamp" },
      { "id": 2, "name": "user_id",    "type": "long" },
      { "id": 3, "name": "event_type", "type": "string" } ]}],   // ← NO partition column here
  "current-schema-id": 0,
  "partition-specs": [{ "spec-id": 0, "fields": [
      { "source-id": 1, "field-id": 1000, "transform": "day", "name": "event_ts_day" }]}],  // the transform
  "default-spec-id": 0, "last-partition-id": 1000,
  "current-snapshot-id": 8910707449840442321,
  "snapshots": [{ "snapshot-id": 8910707449840442321, "sequence-number": 1,
      "manifest-list": ".../snap-8910707449840442321-0-….avro",
      "summary": { "operation": "append", "added-data-files": "2", "added-records": "3",
                   "changed-partition-count": "2", "total-records": "3" } }],
  "refs": { "main": { "snapshot-id": 8910707449840442321, "type": "branch" } },
  "metadata-log": [{ "metadata-file": ".../00000-….metadata.json" }] }  // the CREATE metadata, now superseded
```
Note: the schema holds **only source columns**; `event_ts_day` exists **only as a spec** (`day(event_ts)`,
spec-id 0). Each commit wrote a new `metadata.json`; the catalog pointer moved `00000 → 00001`.

### D.4 Manifest list (Avro, decoded) — points at the manifest + carries partition summaries
```jsonc
{ "manifest_path": ".../e39f46ca-…-m0.avro", "partition_spec_id": 0, "content": 0,   // 0 = data manifest
  "sequence_number": 1, "added_files_count": 2, "added_rows_count": 3,
  "partitions": [{ "contains_null": false,
                   "lower_bound": "0xd2500000", "upper_bound": "0xd3500000" }] }       // day-int range (see D.7)
```

### D.5 Manifest (Avro, decoded) — `data_file.partition` IS the tuple
```jsonc
{ "status": 1,                       // 1 = ADDED
  "snapshot_id": 8910707449840442321,
  "sequence_number": null,           // null = INHERIT from the snapshot's seq number (1)  — see §2
  "data_file": {
    "content": 0,                    // 0 = DATA
    "file_path": ".../event_ts_day=2026-08-25/00000-0-….parquet",
    "partition": { "event_ts_day": "2026-08-25" },       // ← THE PARTITION VALUE lives here, not in Parquet
    "record_count": 2,
    "lower_bounds": [ {"key":1,"value":"0x0028712adc590600"},   // stats keyed by FIELD-ID (1/2/3)
                      {"key":2,"value":"0x0700000000000000"},   // user_id min
                      {"key":3,"value":"0x636c69636b"} ],       // event_type min
    "upper_bounds": [ {"key":2,"value":"0x2a00000000000000"},   // user_id max
                      {"key":3,"value":"0x76696577"} ] } }      // event_type max
// second entry: "partition": {"event_ts_day":"2026-08-26"}, record_count 1
```

### D.6 The Parquet data files — only source columns, raw rows
```
data/event_ts_day=2026-08-25/00000-0-….parquet
   columns: ['event_ts', 'user_id', 'event_type']          # no partition column
   types:   ['timestamp[us]', 'int64', 'string']
   rows:    [{event_ts: 2026-08-25 10:00, user_id: 42, event_type: 'click'},
             {event_ts: 2026-08-25 23:30, user_id: 7,  event_type: 'view'}]
```

### D.7 Decoding the encoded values
| Encoded (in metadata) | Bytes | Decodes to | What it is |
|---|---|---|---|
| manifest-list `lower_bound 0xd2500000` | LE uint32 `0x000050d2` | **20690** | `day('2026-08-25')` = days since epoch |
| manifest-list `upper_bound 0xd3500000` | LE uint32 `0x000050d3` | **20691** | `day('2026-08-26')` |
| manifest `partition.event_ts_day` | Avro int + logical type `date` | **2026-08-25** | why it renders as a date though physically it's 20690 |
| `user_id` bounds `0x0700…` / `0x2a00…` | int64 LE | **7 … 42** | min/max, drives file pruning |
| `event_type` bounds `0x636c69636b` / `0x76696577` | ASCII | **"click" … "view"** | min/max (≤16 chars → stored in full) |
| `event_ts` bound `0x0028712adc590600` | int64 LE | micros since epoch | timestamp min |

### D.8 Truncated stats (a second, focused table)
Separate tiny table `t(id long, url string)` with `write.metadata.metrics.default=truncate(16)`, two rows
sharing a 16-char prefix (`PREFIX0123456789`), both 22 chars long:

| column | true value | **stored** bound (hex → text) |
|---|---|---|
| `url` **lower** | `PREFIX0123456789_alpha` | `…39` → **`PREFIX0123456789`** |
| `url` **upper** | `PREFIX0123456789_zzzzz` | `…3a` → **`PREFIX012345678:`** |
| `id` (numeric) | 1 … 2 | `0x0100…` / `0x0200…` — **exact, never truncated** |

The last byte of the upper bound is the whole point: the prefix's `'9'` (`0x39`) was **incremented to `':'`
(`0x3a`). Two rounding directions keep pruning correct:
- **lower** = 16-char prefix → sorts **≤** the true min (safe: may be too low).
- **upper** = 16-char prefix **then increment the last unit** → sorts **≥** the true max (safe: may be too
  high). `PREFIX012345678:` is greater than every string starting `PREFIX0123456789…`.

Both **widen** the stored range → *may over-include (false-positive file scan), never under-include*. Had
the upper not been incremented (stored `PREFIX0123456789`), the real value `PREFIX0123456789_alpha` — which
is `>` that — would be pruned away and the row lost. The `+1` is what makes truncation safe. (Numeric
types are fixed-width, so `id` is stored exactly.)

### D.9 Two pruning layers — and where bloom filters live
File min/max (D.5/D.7) prune whole **files** from the manifest, *before* opening anything. Once a file is
opened, the engine's **Parquet reader** prunes **row groups** inside it — and that lower layer is where
**bloom filters** live. Bloom filters are a **Parquet file-format feature** (stored in the Parquet file,
offsets in each column-chunk metadata), which Iceberg only *drives* via
`write.parquet.bloom-filter-enabled.column.<col>`; they are **not** in Iceberg metadata (manifests carry
only min/max/null/counts).

```
Iceberg manifests   →  prune whole FILES        (partition tuple + per-file min/max/null;  before open)
      │  surviving files → engine's Parquet reader
      ▼
Parquet internals   →  prune ROW GROUPS/pages   (row-group min/max, page index, dictionary, BLOOM FILTER)
```
A bloom filter has **no false negatives** ("definitely absent" is always true), so skipping a row group on
that answer is safe — the same conservative, never-under-include principle as truncated bounds, one layer
down. Write your data as ORC/Avro instead and Parquet bloom filters simply don't exist for it — it's a
property of the chosen file format.

---
*Offline companion to `iceberg-online-lab-and-resources.md`. Written against Iceberg 1.11.0 (v2/v3).
Cross-warehouse specifics reflect ~2025–2026 and should be re-verified against current vendor docs.*

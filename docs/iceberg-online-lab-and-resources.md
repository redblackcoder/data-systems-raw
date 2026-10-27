# Iceberg — Online Lab & Resource Hub (connected companion)

> **Use this when you have internet.** It's the hands-on + link-heavy half of the pair. The other file,
> `iceberg-deep-dive.md`, is the fully-offline conceptual read for camping. Anchored to **Iceberg 1.11.0**
> and the live REST OpenAPI spec.
>
> Contents: **A.** curated resources · **B.** build a REST catalog (2-day MVP) · **C.** vendor docs to
> verify the interop section of the offline read.

---

## A. Curated resources (annotated)

### Official / normative — read first
- **Iceberg docs (latest, 1.11.0)** — `iceberg.apache.org/docs/latest/` → *Concepts → Tables*, *Catalogs*,
  *Maintenance*, *Spark/Flink*.
- **Table Spec** — `iceberg.apache.org/spec/`. The normative source of truth.
- **View Spec** — `iceberg.apache.org/view-spec/`.
- **REST Catalog OpenAPI** — `github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml`.
  *The single most important artifact for your depth area.* Read every path + the `CommitTableRequest`
  (requirements/updates) schema. Paste the raw YAML into a Swagger UI to browse it.
- **AWS catalog docs** — `iceberg.apache.org/docs/latest/aws/` (Glue **and** the built-in `DynamoDbCatalog`).

### Books & long-form
- **"Apache Iceberg: The Definitive Guide"** — Dremio/O'Reilly, free PDF from Dremio. Best single text.
- **Jack Vanlightly — "Understanding Apache Iceberg" series** — `jack-vanlightly.com`. High-signal on the
  *why* of the metadata tree, commit concurrency, log-structured design.

### Blogs / deep-dives
- **Dremio blog** (Alex Merced, Dipankar Mazumdar): partitioning, COW vs MOR, compaction, catalog comparisons.
- **Tabular blog / talks** (Ryan Blue & the original authors; Tabular is now part of Databricks).
- **Starburst/Trino, Snowflake, Cloudera** engineering blogs for engine-side internals.

### Catalog implementations — read the code (your differentiator)
- **Apache Polaris** — `polaris.apache.org`, `github.com/apache/polaris`. OSS REST catalog (Java, from
  Snowflake). Production reference for the REST protocol + RBAC + credential vending.
- **Lakekeeper** — `github.com/lakekeeper/lakekeeper`. Rust REST catalog; clean from-scratch reference.
- **Project Nessie** — `projectnessie.org`. Git-like catalog (branches/tags/merge) that speaks REST.
- **Unity Catalog (OSS)** — `unitycatalog.io`. Multi-format; implements the Iceberg REST surface.
- **Apache Gravitino** — `gravitino.apache.org`. Federated multi-catalog metadata layer.
- **Iceberg's own `RESTCatalogServer` / `CatalogHandlers`** in `iceberg-core` — the shortcut for §B.

### Hands-on / quickstarts
- **Spark quickstart (Docker)** — `iceberg.apache.org/spark-quickstart/`. `docker compose` with
  spark-iceberg + a REST catalog + MinIO. Fastest way to see the whole stack.
- **PyIceberg** — `py.iceberg.apache.org`. Talk to a REST catalog from Python, no JVM.

### Talks / video
- **Iceberg Summit** and **Subsurface** archives on YouTube — search "Iceberg REST catalog", "Iceberg v3",
  "deletion vectors". Ryan Blue's design talks especially.

### Community
- Iceberg **Slack** + `iceberg.apache.org/community/`; the **dev mailing list** (v3 / REST proposals).

> ⚠️ WebSearch was unavailable when this was compiled, so links are canonical titles/domains verified
> against the docs (v1.11.0) and the REST OpenAPI. If a deep link 404s, search the title.

---

## B. Build an Iceberg REST Catalog — 2-day MVP 🔨

**Goal:** a spec-compliant REST catalog that **Spark** and **PyIceberg** can create namespaces/tables in,
append data to, and commit to concurrently.

### B.1 Scope
**Must-have endpoints:** `GET /v1/config`; namespaces create/list/load/`HEAD`/drop; tables
create/load/**commit**/drop/`HEAD`/list; `POST /v1/tables/rename`.
**Cut for MVP** (say this out loud — scoping *is* the signal): views, functions, multi-table transactions,
server-side scan planning (`/plan`,`/tasks`), credential vending & remote signing, metrics, pagination,
nested namespaces, full OAuth (use a static bearer token).

### B.2 The insight that makes it 2 days, not 2 weeks
The catalog's real job is **(a) store table pointers and (b) atomically CAS the pointer on commit after
validating requirements**. You do **not** need to hand-write metadata manipulation — `iceberg-core` ships
**`CatalogHandlers`**, which implements the REST request→response semantics (requirement validation +
update application) on top of *any* `Catalog`. Iceberg's tests wrap this in an `RESTCatalogServer`.

> **Recommended path:** a JVM app that exposes the HTTP routes and delegates each to `CatalogHandlers`
> over a backing `Catalog`. Start with `JdbcCatalog` (Postgres) or `InMemoryCatalog`; storage = MinIO/S3.

### B.3 Stack & data model
- **Stack:** Java/Kotlin + Javalin or Spring Boot. Deps: `iceberg-core`, `iceberg-aws` (S3FileIO), Jackson.
- **Pointer store:** Postgres via `JdbcCatalog` (Day 1). Row ≈ `(catalog, ns, table) → metadata_location,
  previous_metadata_location, uuid`.
- **Object store:** MinIO locally (S3-compatible). Engines write metadata/data files there; the catalog
  tracks only the pointer.
- **Commit = CAS:** `UPDATE … SET metadata_location=:new WHERE table=:t AND metadata_location=:expected`;
  0 rows ⇒ conflict ⇒ **409**.

### B.4 Hour-by-hour
**Day 1 — a working REST catalog**
1. HTTP skeleton; implement `GET /v1/config` → `{ "defaults":{}, "overrides":{}, "endpoints":[…] }`.
2. `docker compose` MinIO + Postgres; configure `S3FileIO` + `JdbcCatalog`.
3. Route namespace + table endpoints to `CatalogHandlers.*`; map Iceberg exceptions → REST error JSON +
   status (404 NoSuchTable, 409 CommitFailed/AlreadyExists, 400 bad update).
4. Connect **PyIceberg** (`type=rest, uri=http://localhost:8080`) → create namespace + table.
5. Connect **Spark** (`SparkCatalog`, `type=rest`, `uri=…`) → `CREATE TABLE`, `INSERT`, `SELECT`,
   time-travel. **Milestone: end-to-end append.**

**Day 2 — depth + correctness**
6. Implement your **own** DynamoDB-backed `Catalog` + `TableOperations` where `commit()` does a DynamoDB
   **conditional write** (CAS on `metadata_location`). Swap it behind the same routes.
7. Add `rename`, `drop(purge)`, namespace properties, `HEAD` existence checks.
8. Static **bearer-token** auth filter + per-token `prefix`/warehouse scoping.
9. **Concurrency test:** two writers off the same base → one succeeds, one gets 409, retry re-plans and
   succeeds. This demo *proves* you understand the protocol.
10. Stretch: `stage-create` (atomic CTAS), a `LoadTableResult.config` override, basic credential vending.

### B.5 Deeper-learning variant
Implement the protocol from scratch in **Python/Go/Rust** (you then apply `MetadataUpdate`s yourself to
build the new `metadata.json`). Read **Lakekeeper** (Rust) as the reference. Great stretch; risky as the
primary 2-day plan.

### B.6 Test loop
- **PyIceberg** for fast iteration: `load_catalog("demo", **{"type":"rest","uri":...})`.
- **Spark** for real writes + `MERGE` + time-travel.
- Validate status codes against the OpenAPI; the `assert-*` requirement handling + 409-on-conflict are the
  correctness core.

---

## C. Vendor docs — verify the offline read's interop section (Part X)

The offline `iceberg-deep-dive.md` covers cross-warehouse metadata exchange from knowledge (dated
~2025–2026). Confirm current specifics here:
- **Snowflake Iceberg tables** — `docs.snowflake.com/en/user-guide/tables-iceberg` (managed vs
  catalog-integration; **Snowflake Open Catalog** = managed Polaris).
- **Apache Polaris** — `polaris.apache.org`.
- **Databricks UniForm** — `docs.databricks.com/…/delta/uniform`; **Unity Catalog Iceberg REST** (read-only,
  public preview).
- **AWS S3 Tables** — `docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html` (managed Iceberg +
  Glue Data Catalog integration + auto-maintenance; Athena/EMR/Redshift).
- **BigQuery Iceberg** — `cloud.google.com/bigquery/docs/iceberg-tables` + **BigLake metastore**.
- **Redshift** — Spectrum reading Iceberg via the Glue Data Catalog.

---
*Pair with `iceberg-deep-dive.md` (offline). §B anchored to Iceberg 1.11.0 + the REST OpenAPI; §C facts
spot-checked against vendor docs.*

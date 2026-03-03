# Data Documentation App

Internal app for browsing and understanding MNTN's datasets.
Demo recording: `documentation/misc/zoom_recordings/zoom_2026_01_26_gmt/` (Jan 26 2026).

---

## What It Does

- Indexes ~3,000 datasets across BigQuery
- Provides **curated topic views** (e.g., "Impressions") with descriptions, key columns, and subtopic organization
- Shows table schema (columns, types — useful for spotting BIGNUMERIC columns that need casting)
- Shows lineage (upstream/downstream tables) and an ER diagram
- Shows a data preview (sample rows)
- Marks tables as **supported** or **unsupported** (see below)
- Has a **FAQ/Q&A feature** — ask questions about a topic; upvote questions to notify the topic owner

---

## Supported vs. Unsupported Tables

Every table is either **supported** or **unsupported**:

- **Supported** = the table is linked to a topic via a SQLMesh tag
- **Unsupported** = the table exists but has not been tagged into any topic

This is useful for immediately filtering out dev/staging tables and identifying the canonical tables for a use case. When you find a table you want to use, check whether it's supported first.

---

## How Topics Work (and How Tables Get Linked)

Topics (e.g., "Impressions") are defined in a YAML file. Each topic has:
- A title and description
- Subtopics (e.g., "Raw tables", "Pipeline staging", "Aggregated", "User-facing")
- Each subtopic specifies a **tag** — not a manual list of tables

```yaml
title: Impressions
description: ...
subtopics:
  - title: Raw tables
    tag: impressions_raw
  - title: Aggregated tables
    tag: impressions_aggregated
```

Tables are associated with subtopics by adding the corresponding tag to the SQLMesh table definition. Example in a `.sql` SQLMesh model:

```sql
MODEL (
  name logdata.impression_log,
  tags ['impressions_raw'],
  ...
);
```

This means:
- **Topic YAML** = manually maintained, canonical, slow to change
- **SQLMesh tags** = part of the regular development workflow; can be linted and reviewed in PRs
- If a table has no matching tag, it's unsupported

---

## FAQ Feature

Within each topic, there is an aggregated FAQ. You can:
- Browse questions others have asked about that topic
- Upvote a question — this triggers a notification to the topic owner to provide a canonical answer
- Ask new questions

The intent is to capture Slack-thread knowledge in a durable, searchable place.

---

## Key Observations

- Dev tables are excluded from search results (filtered by naming convention)
- Schema view shows column types — useful for identifying BIGNUMERIC columns before writing queries
- Last-updated timestamp is visible — a stale date is a signal the table may be inactive or deprecated
- The lineage view shows upstream and downstream tables (moderately useful, improving over time)

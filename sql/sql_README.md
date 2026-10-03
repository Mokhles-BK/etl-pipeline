# SQL schemas

## staging_schema.sql
Landing zone for raw-ish, validated records straight from the source. One
table per entity (`staging.earthquakes`), a durable per-entity incremental
cursor (`staging.load_cursors`), and rejected rows (`staging.load_errors`).
Idempotent via `ON CONFLICT (event_id) DO UPDATE` — see loader.py.

## warehouse_schema.sql
Star schema built from staging, meant for analytics/dashboard queries rather
than operational writes.

**Grain:** one fact row per earthquake event (`fact_earthquake_events`),
matching staging's grain exactly — no aggregation happens at load time, only
at query time.

**Dimensions:**
- `dim_time` — one row per calendar date seen in the data. `date_key` is an
  integer `YYYYMMDD` (e.g. `20260923`), the standard warehouse convention:
  sorts naturally as an integer, joins without timezone ambiguity once the
  date component is extracted from `event_time` (epoch ms).
- `dim_location` — **not** one row per exact coordinate (that would make the
  dimension as large as the fact table and defeat the point of a dimension).
  Instead: `region` is parsed from USGS's free-text `place` field (the text
  after the last comma, e.g. "Indonesia" from "126 km NNE of Teluknaga,
  Indonesia"), and lat/lon are floored to whole-degree buckets. This groups
  nearby events for meaningful aggregation ("events per region", "events per
  ~111km grid cell") without one dimension row per unique event.
- `dim_magnitude_type` — the small set of USGS magnitude scales (`mww`, `mb`,
  `ml`, etc.) as a lookup table, so a dashboard can filter/group by scale
  without repeating the string in every fact row.

**Indexing:** `fact_earthquake_events` is indexed on each foreign key
(`date_key`, `location_key`, `mag_type_key`) plus `mag` directly, since those
are the columns a dashboard will filter or group by. No index on `depth`,
`sig`, etc. — those are typically read, not filtered/grouped on, and an
unused index only costs write time on every transform run.

**Why dimensions use `ON CONFLICT DO NOTHING` but the fact table uses
`ON CONFLICT ... DO UPDATE`:** a dimension row (a date, a region+bucket, a
magnitude type) doesn't change once it exists — "2026-09-23" is always
"2026-09-23". A fact row can change: USGS revises magnitude/location
estimates for an event after initial reports, so re-running the transform
needs to update the existing fact row with the latest values, not skip it.

**Transform is watermark-free by design** (see `warehouse.py`'s module
docstring for the tradeoff) — it re-reads all of `staging.earthquakes` on
every run rather than tracking its own cursor. This is simple and correct at
the current data volume; if staging grows large enough that a full scan
becomes expensive, add a `warehouse.transform_watermark` table (same pattern
as `staging.load_cursors`) and filter the transform's queries on
`staging.earthquakes.loaded_at > watermark`.

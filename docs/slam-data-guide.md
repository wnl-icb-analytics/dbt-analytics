# SLAM Data: Source-to-Staging Methodology

How the four SLAM contract-monitoring feeds (ACM, PLCM, DrPLCM, DePLCM) are transformed between a provider's submission and the tables in `STAGING`. Each stage states the problem it addresses and the method applied.

## The pipeline

```
1. Provider file            spreadsheet/CSV, layout varies by provider/month/revision
        │
        ▼
2. WNL_ServicesDataLocal    values stored positionally as Col1..ColN;
   (ISL loader)             column meanings stored separately as a T-SQL string
        │                   per file in Log_ProcessingEvent.SourceColumnHeaders
        ▼
3. Layout resolution        every distinct layout fingerprinted (md5) into
   (DATA_LAKE.SDL pipeline) META_SCHEMA_VERSIONS; each file matched to its layout
        │                   (LSPLCM alone: 101 layouts across 18,583 files)
        ▼
4. <feed>_raw views         positions resolved to named columns -
        │                   one CASE branch per layout per column
        ▼
5. <feed>_mapped views      canonical column names (COALESCE across provider
        │                   spelling variants) + TRY_CAST data types
        ▼
6. DATA_LAKE.SDL.<FEED>     materialised tables, loaded nightly, with the META_*
        │                   tables tracking files, exceptions and row coverage
        ▼
7. STAGING.<FEED>.STG_*     dbt staging: cleaned dv_ fields, sparse columns
        │                   dropped, tested on every refresh
        ▼
8. STG_*_LATEST views       current provider statement per (provider, FY, month) -
                            the reporting entry points
```

Stages 1–6 are the platform pipeline (Snowflake-Deployment repo, `services_data_local`). Stages 7–8 are this dbt project.

## 2–3. Positional storage and layout resolution

**Problem.** Provider files do not share a column layout — it varies by provider, by month, and by revision within a month. The ISL loader handles this by storing every file's values positionally (`Col1..ColN`) in `Master_<feed>_Data`; the table itself carries no column meanings at all. What each position meant is recorded per file in a different table, `Log_ProcessingEvent.SourceColumnHeaders`, as the literal T-SQL select fragment the loader used:

```
[Col1] AS [FinancialMonth], [Col2] AS [CCG], [Col3] AS [Provider Code], ...
```

That string is the only record of each file's schema. The data is unreadable without it: there is no header row, no metadata table, no convention — the schema of a 600M-row table exists as SQL text inside a log column. Reconstructing usable tables from this is a four-step recovery.

**Step 1 — parse the T-SQL.** Each `SourceColumnHeaders` string is parsed (`[Col(\d+)] AS [([^\]]+)]`) into (position → header) pairs. The headers are whatever the provider typed into row 1 of their spreadsheet, so the extracted names then go through identifier sanitisation. The sanitiser's special cases are an inventory of what actually arrived in provider headers: columns named just `%`, `#` or `&`; embedded `+`, `<`, `>`, `=`, `*`, `?`, `!`, `@`, `$` and `|`; `...` run-ons; brackets, colons, slashes and repeated whitespace; names that collide with SQL reserved words; names starting with a digit. Each gets a deterministic rewrite (`%` → `percent`, `<` → `_lt_`, camelCase split at acronym boundaries) so every header becomes a stable unquoted snake_case identifier.

**Step 2 — fingerprint layouts.** The full mapping string is md5-hashed; every distinct layout becomes one `version_id` in `META_SCHEMA_VERSIONS`. The same layout always produces the same id, however many files use it. LSPLCM has 101 distinct layouts across 18,583 files; the four SLAM feeds together have 236.

**Step 3 — match every file to a layout** in `META_FILE_VERSIONS`:

- Primary: the file's header string equals a known layout, byte for byte. The matching is fragile by nature — provider headers contain `£` and `&`, which deployment tooling mangles between codepages, so the layout strings are round-tripped base64-encoded to keep the comparison exact. One altered character and the file never matches.
- Fallbacks, for files whose log row has no headers at all (a common loader behaviour for re-sent files): same `HeaderID` as a matched sibling file, then same `ProfileCode` (same layout family). `MATCH_METHOD` records which path resolved each file. These cascade-matched files can be loaded long after newer ones, which is why file ids are not in load order.
- No match: the file is excluded and listed in `DATA_LAKE.SDL.META_UNMAPPED_FILES` with a reason.

**Step 4 — generate the resolution views** (next section). All four steps are code-generated from the metadata, not hand-maintained: when a provider invents a new layout, the pipeline re-derives everything.

**Coverage.** At the time of writing, 3 SLAM files (of ~50,000 loaded) are unmapped — all `data_not_logged`, where the log row exists but the data rows never arrived upstream. New files appear briefly as `not_yet_refreshed` until the nightly refresh. `DATA_LAKE.SDL.META_ROW_COUNT_COMPARE` shows source-vs-loaded row coverage per feed (>99.999%).

## 4–5. Position-to-name resolution and canonical mapping

The `<feed>_raw` views make the recovery executable. Each row joins to its file's `version_id`, and every output column is a CASE over versions stating where that column lives in each layout. From the LSPLCM view:

```sql
CASE
    WHEN fv."VERSION_ID" IN ('72a5f19843d37d2d') THEN d."Col49"
    WHEN fv."VERSION_ID" IN ('5dca51a88420d9a7') THEN d."Col32"
    WHEN fv."VERSION_ID" IN ('150ecfb64c560215') THEN d."Col46"
END AS activity_actual
```

The same column arrived in position 49, 32 or 46 depending on the layout. Rows from layouts that never contained the column are NULL. The full LSPLCM view is ~580 columns and 944 CASE expressions, generated from the metadata and regenerated whenever a new layout appears.

The `<feed>_mapped` views reconcile naming drift: where providers spelled the same concept differently across layouts (`ADHOC_ITEM_CODE` vs `ADHOCITEM_CODE`), a curated mapping declares the canonical name and the view COALESCEs the variants into it, applying TRY_CAST data types where declared.

### The control tables, and where to intervene

The whole resolution is driven by inspectable tables in `DATA_LAKE.SDL` — when a value looks wrong, these are the levers:

| Object | What it holds | When to look at it |
|---|---|---|
| `CONTROL_COLUMN_MAPPING` | Source alias → canonical name (+ optional data type). The deployed copy of the curated mapping CSV (`mappings/name_mapping.csv` in Snowflake-Deployment) | A column is misnamed, two variants aren't being COALESCEd, or a type cast is wrong — the fix is a CSV row, not SQL |
| `META_SCHEMA_VERSIONS` | Every distinct layout per feed, with its raw header mapping | Check what a specific layout said a position meant |
| `META_FILE_VERSIONS` | File → layout, with `MATCH_METHOD` (`headers` / `header_id` / `profile_code`) | A file's values look shifted — confirm it matched the right layout, and how |
| `META_FILE_REGISTRY` | Per-file metadata: original file name, row count, load timestamp | Trace a row back to the file it came from (`meta_file_id` + `meta_batch_id`) |
| `META_EXCEPTIONS` | Files whose headers matched no known layout | A provider invented a new layout — it sits here until mapped |
| `META_BUILD_STATE` | Per-feed incremental high-water mark + `NEEDS_REBUILD` flag | The final table lags the views, or a mapping change needs a rebuild |
| `META_NULL_RATE_PROFILE` | Null rate per (feed, column) | Judge whether a sparse column is worth requesting in staging |

The repair loop when something is wrong: diagnose with `META_FILE_VERSIONS`/`META_SCHEMA_VERSIONS` (did the file match the right layout?), fix the mapping in the CSV if it's a naming/typing problem, regenerate and redeploy the views (`generate.py` in Snowflake-Deployment), and let the nightly task rebuild the affected feed — `NEEDS_REBUILD` flips automatically when the view schema drifts from the table, or `SP_BULK_BACKFILL('<FEED>')` forces it immediately. New layouts follow the same loop: `audit.py <FEED>` lists the unmapped source columns, the CSV gains rows, and the pending files in `META_EXCEPTIONS` resolve on the next refresh.

Because every file carries `meta_file_id` and `meta_batch_id` all the way into STAGING, any individual staged row can be traced to its source file, layout and match method.

## 6. DATA_LAKE.SDL tables

Materialised nightly from the `_mapped` views. Minimal cleaning by design: canonical column names, positions resolved, very sparse columns removed. Values remain provider text — costs with `£` and commas, free-text dates and financial years pass through unchanged. DATA_LAKE preserves what arrived.

## 7. STAGING tables (dbt)

Grain stays 1:1 with `DATA_LAKE.SDL` — same rows, no joins to other data, no business logic.

| Field | Method | Failure behaviour |
|---|---|---|
| `dv_financial_year` | Validated to `'YYYYYY'`; accepts `202526`, `2025/26`, `2025-26`, `2025-2026`; second year must follow the first | Junk (`215551`) and ambiguous bare years (`2020`) → NULL, then the FY token in the platform file name (`PLCM_2627_InformationStandard...`), then derived from the activity date (PLD feeds — see below) |
| `dv_financial_month` | Whole number 1 (April) – 12 (March) | Junk and fractional values → NULL, then derived from the activity date (PLD feeds) |
| `dv_financial_period_source` | Records per row which of the three sources supplied the period: `stated`, `file_name` or `activity_date` | NULL when the period is unrecoverable |
| `dv_total_cost` and all price/activity/quantity fields | Parsed to `NUMBER(38,6)`: strips currency symbols and thousands commas; accounting-style `(1,234.56)` → negative | Non-numeric text (`TBC`) → NULL |
| `dv_dataset_created_at` | Provider's `DATE_AND_TIME_DATA_SET_CREATED` parsed across every format found in profiling (ISO, UK, US AM/PM, Excel serial numbers, several broken variants) | Unparseable values → NULL |
| `dv_provider_code` | ODS code cleaned via the Dictionary: site-suffixed codes that are not valid org codes resolve to the parent (`RAS00` → `RAS`) | — |
| Activity/clinical dates | Parsed from UK date formats | Unparseable → NULL |
| Column pruning | Columns <5% populated dropped (LSPLCM: 580 → ~60) | Originals remain in DATA_LAKE.SDL |

**Activity-date period derivation (PLD, Drugs, Devices).** Providers bill by activity date, so when the stated period and the file-name token both fail, the period is derived from the feed's activity date: PLD uses the activity end/start dates (including the plain-named columns used by pre-Sep-2021 layouts), Drugs the dispensed then delivery date, Devices the implantation then insertion date. The rule was validated before adoption: where both a stated period and an activity date exist, the months agree at 99.86% (PLD, 531m rows), 99.85% (Drugs) and 98.8% (Devices), with year-and-month jointly matching at 99.71% (Drugs); the drug delivery date agrees 100%. The derived periods were also checked against the independent load timestamp: 0.00% are logically impossible (activity after the file was loaded) vs 0.03% of provider-stated periods, with a tighter lag distribution — the derivation is measurably cleaner than the field it backfills around. Dates are gated to plausible values (April 2015 – today) so junk cannot create phantom periods. ACM is excluded — it is an aggregate feed with no activity dates. `dv_financial_period_source` makes every row's provenance visible.

The governing rule: invalid values become NULL, never guesses — and derivations are validated, gated and labelled. Originals are retained in a `*_raw` column alongside the `dv_` field, or in DATA_LAKE.SDL.

The `dv_` names are an established interface for this feed family. New model
families should prefer an unprefixed canonical business name, but enhancements
should not rename these fields merely to remove the prefix.

Staging does not validate code columns (POD, service, TFC pass through as submitted), join names onto codes, or apply reporting logic.

## 8. Latest-submission resolution (`_LATEST` views)

**Problem.** SLAM files are cumulative year-to-date restatements (typically 4–6 months per file), with no marker for which submission is current. Summing the staging tables directly double-counts roughly 8x. The provider-populated `DATE_AND_TIME_DATA_SET_CREATED` cannot order submissions reliably: ~10% of files contain more than one value in the field, and same-day resubmissions are ambiguous. `meta_file_id` is not monotonic with load time (~45% of consecutive loads have inverted ids, mainly cascade-matched back-dated files).

**Method.** Two dates do different jobs:

- **Which month a row belongs to** comes from the data itself — `dv_financial_year` / `dv_financial_month` as stated inside the file. Arrival timing never assigns periods.
- **Which file wins when two files state the same month** is decided by the platform processing log (`META_FILE_REGISTRY.CREATED_DATETIME` — when ISL loaded the file), with file/batch id tiebreaks.

Resolution is per reporting month, not per file: for each provider and financial year, every stated month independently selects the most recently loaded file containing data for that month. Consequences:

- A full-year restatement supersedes all months it states.
- A submission split across files (M1–6 + M7–12) keeps both halves.
- A single-month correction replaces only that month.

The winning file per slice is published in `STAGING.SLAM.STG_SLAM_LATEST_SUBMISSION` (feed × provider × FY × month → file). The `_LATEST` views join the staging tables to it.

## Known limitations

1. **Rows with NULL `dv_financial_year`/`month`** — no stated period, no file-name token and no usable activity date. These rows fall out of month-level reporting; raw values remain in `financial_year_raw` / `financial_month_raw`, and `dv_financial_period_source` is NULL. (Before the activity-date fallback this was ~2% of PLD — 753 pre-Sep-2021 files whose layouts had no period columns at all — and ~0.4% of Drugs; the fallback recovers roughly two-thirds of the PLD gap.)
2. **74 cost values (of 51.6m populated, Drugs) do not parse** — true junk, NULL in `dv_total_cost`.
3. **Backloaded history ordering**: for ~340 provider-months (none in 26/27, concentrated 20/21–23/24), load order and the provider-stated creation date disagree about which file is latest. The views follow load order.
4. **Upstream rebuilds rewrite history**: if the platform pipeline rebuilds a feed (schema drift), the staging tables are rebuilt from it and figures can change. Nightly tests flag grain breaks.
5. **Unmapped files**: anything in `META_UNMAPPED_FILES` is absent from all downstream layers. Currently 3 SLAM files, all missing their data upstream rather than awaiting mapping.

## Verification queries

```sql
-- parse coverage: source cost values vs cleaned
select count(t.total_cost) as src, count(s.dv_total_cost) as cleaned
from DATA_LAKE.SDL.LSDRPLCM t
join STAGING.SLAM.STG_LSDRPLCM s on s.meta_sk_row_id = t.meta_sk_row_id;

-- winning files for a period
select meta_file_id from STAGING.SLAM.STG_SLAM_LATEST_SUBMISSION
where feed = 'LSACM' and dv_financial_year = '202526' and dv_financial_month = 12;

-- rows excluded from month-level reporting (unrecoverable periods)
select financial_year_raw, count(*) from STAGING.SLAM.STG_LSDRPLCM
where dv_financial_year is null group by 1 order by 2 desc;

-- restatement collapse: history vs latest
select (select count(*) from STAGING.SLAM.STG_LSDRPLCM) as history_rows,
       (select count(*) from STAGING.SLAM.STG_LSDRPLCM_LATEST) as latest_rows;
```

Column descriptions in Snowsight state what each derived field was parsed from and how failures behave. The cleaning code is in `macros/transformations/parse_slam.sql` and `models/staging/commissioning/slam/`.

# Configuration Macros

Centralised configuration for campaigns, QOF, and observability settings.

## Campaign sets

The COVID and flu models build every campaign in `covid_reported_campaign_ids()` /
`flu_reported_campaign_ids()`:

```sql
WITH all_campaigns AS (
    {{ covid_reported_campaigns() }}
)
```

Campaigns are added to those lists and never removed, so a season that has been reported
keeps its rows when the next one is added. Its population is resolved as at the campaign
by `int_covid_flu_campaign_population`. Clinical eligibility is still recomputed on every
build, so a retrospectively entered code can still move a closed season.

For a single campaign's parameters, use `covid_autumn_config()`, `flu_current_config()`
and their siblings, or call `covid_campaign_config('COVID Autumn 2026')` directly. Dates
live in `macros/campaigns/*_campaign_config.sql` only.

## Terminology versions

Each campaign carries `terminology_version`, the PRIMIS SCT codeclusters workbook version
the season was reported under (the last release on or before its RUN_DAT or final
AUDITEND_DAT). The cohort intermediates read clusters with
`get_observations(..., versioned=true)` from `stg_reference_ukhsa_codecluster_versions`
and filter on that version, so a closed season is reproduced with the clusters it was
published with while the current season uses the current ones. `COMBINED_CODESETS` holds
only the current version and is untouched.

## Rebuilding closed seasons

Closed seasons are recomputed monthly, not daily. The cohort intermediates are incremental
(`delete+insert` on `campaign_id`) and take their `all_campaigns` CTE from
`covid_build_campaigns()` and `flu_build_campaigns()`, which return only the seasons in
flight on a routine run. A monthly run restates every campaign against the current source
data and its pinned terminology:

`sh
dbt build --select tag:covid_flu+ --full-refresh
# or, without dropping the tables
dbt build --select tag:covid_flu+ --vars '{covid_flu_rebuild_closed: true}'
`

## QOF

```sql
{{ qof_reference_date() }}

-- QOF register
WHERE clinical_effective_date <= {{ qof_reference_date() }}
```

## Adding a campaign year

1. Add the campaign block to `flu_campaign_config()` or `covid_campaign_config()` with the
   dates from that year's UKHSA specification. Set `terminology_version` to the PRIMIS
   codeclusters version the season is reported under, and load that version into
   `DATA_LAKE__NCL.TERMINOLOGY.UKHSA_CODECLUSTER_VERSIONS` (and the `*_LATEST` table if it
   is the current season).
2. Append the campaign id to `flu_reported_campaign_ids()` or
   `covid_reported_campaign_ids()`.
3. Point the season-in-flight selectors at the new season. These name the current season
   for models that need it; they do not decide which campaigns get built:
   - `flu_campaign_selection.sql` → `flu_current_campaign()`, `flu_previous_campaign()`
   - `covid_campaign_selection.sql` → `covid_current_autumn()`, etc.
4. Add the campaign to the label and sort expressions in `int_covid_flu_dashboard_current`,
   `int_covid_flu_dashboard_wide` and `covid_flu_dashboard_base`, to the `campaign_year`
   mapping in `fct_covid_flu_uptake`, and to the `campaign_id` comment in
   `sem_olids_vaccinations`.

QOF has its own reference date in `qof_config.sql` → `qof_reference_date()`.

# Expanded OLIDS observations

The upstream observation table combines recorded codes from observations,
allergies and referral requests. Analysts keep using the existing ID, code,
label and date fields. Source-table placement is provenance, not a separate
clinical eligibility rule. Existing code-list and date rules remain unchanged.

## Fields available to consumers

`stg_olids_observation.id` is populated and unique across the combined feed.
Native observation IDs remain unchanged. Added records have deterministic UUIDs,
so the same source record receives the same observation ID on every rebuild.
The upstream profile found one original ID shared by different source entities;
using the original IDs unchanged would not preserve observation ID uniqueness.

Raw and staging expose these upstream fields:

- `source_entity`: original delivery table.
- `source_record_id`: original UUID within that table.
- `allergy_medication_name`: medication name recorded on an allergy record.

The source fields are available for explicit use in staging. They are not added
to the shared macro outputs. `get_observations` keeps its existing `id` field;
`get_ltc_lcs_observations` and `get_ltc_lcs_observations_latest` keep
`observation_id`. Each macro adds only `allergy_medication_name`, which is clinical
content rather than tracking metadata. A consumer needing source provenance can
join its observation ID back to staging.

Staging continues to exclude deleted records and records without a person ID.
Measurement fields and problem flags remain null where the added source has no
applicable value. No values or dates are invented. Programme models and published
outputs, including C-LTCS and Valproate, are unchanged by this PR.

## Validation and publication

[dbt-OLIDS PR 298](https://github.com/wnl-icb-analytics/dbt-OLIDS/pull/298)
owns the combined population and its publication. It must publish the new columns
before this staging change is built. Existing analytics can already consume the
added rows through its original column projections; this PR exposes the additional
fields without changing those existing columns.

The raw model is regenerated with the established source generator. Source YAML
and staging explicitly project the new fields. Validate in the existing `dev`
target and shared `DEV__` layers after upstream publication. The upstream
[full profile](https://github.com/wnl-icb-analytics/dbt-OLIDS/blob/main/docs/expanded-observation-completeness.md)
contains row preservation, completeness and code-list checks.

Programme corrections are separate work. [Valproate issue 1126](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1126)
tracks the existing Valproate
union will duplicate referral event rows and IDs after upstream expansion, while
its referral-present flags and earliest/latest dates are unaffected by duplicate
copies. The foot-screening interpretation defect is tracked in
[issue 1125](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1125).

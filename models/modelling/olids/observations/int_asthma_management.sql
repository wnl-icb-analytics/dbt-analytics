-- depends_on: {{ ref('stg_olids_observation') }}
{{
    config(
        materialized='table',
        tags=['cltcs'])
}}

/*
Per-person asthma-management feature flags.

The flag logic now lives in the reusable asthma_management_history() macro, so the same
definition can be reconstructed as-at a past date (used by cltcs_monthly_covariates). This
model is the "current" caller: as_of = current_date().

Note vs the previous version: the macro applies a consistent upper date bound (<= as_of), so
observations/orders dated AFTER today no longer count. Output is identical except for any
patient with future-dated records, which are now (correctly) excluded.
*/

select * from (
    {{ asthma_management_history(as_of="current_date()") }}
)

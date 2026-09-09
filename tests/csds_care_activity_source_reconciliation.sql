-- Compare occurrence keys so missing and unexpected rows cannot cancel in a count.
with source_keys as (
    select unique_submission_id as submission_id, unique_care_activity_identifier as activity_id, 1 as is_present
    from {{ ref('stg_csds_care_activity_history') }}
), fact_keys as (
    select submission_id, activity_id, 1 as is_present
    from {{ ref('fct_csds_care_activity') }}
)
select
    coalesce(s.submission_id, f.submission_id) as submission_id
    , coalesce(s.activity_id, f.activity_id) as activity_id
    , iff(s.is_present is null, 'unexpected_in_fact', 'missing_from_fact') as discrepancy
from source_keys as s
full outer join fact_keys as f
    on s.submission_id = f.submission_id
    and s.activity_id = f.activity_id
where s.is_present is null or f.is_present is null

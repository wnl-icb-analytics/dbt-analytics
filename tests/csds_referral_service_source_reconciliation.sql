-- Retain every referral/team key, including the unspecified team, and no extra keys.
with source_keys as (
    select unique_service_request_identifier as referral_id, care_professional_team_local_identifier as local_team_id, 1 as is_present
    from {{ ref('stg_csds_service_type_history') }}
    group by unique_service_request_identifier, care_professional_team_local_identifier
), fact_keys as (
    select referral_id, local_team_id,
        1 as is_present
    from {{ ref('fct_csds_referral_service') }}
)
select
    coalesce(s.referral_id, f.referral_id) as referral_id
    , coalesce(s.local_team_id, f.local_team_id) as local_team_id
    , iff(s.is_present is null, 'unexpected_in_fact', 'missing_from_fact') as discrepancy
from source_keys as s
full outer join fact_keys as f
    on s.referral_id = f.referral_id
    and s.local_team_id is not distinct from f.local_team_id
where s.is_present is null or f.is_present is null

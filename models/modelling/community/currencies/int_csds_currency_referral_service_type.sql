{{ config(materialized='table') }}

-- One service/team type per referral for the community currency grouping,
-- which classifies a contact by the team type of its referral. Referrals with
-- several CYP102 teams keep the newest reported relationship. General
-- reporting keeps every team in fct_csds_referral_service.
select
    unique_service_request_identifier
    , person_id
    , care_professional_team_local_identifier
    , service_or_team_type_referred_to_community_care as team_type_code
    , organisation_code_provider
    , unique_submission_id
    , reporting_period_end_date
from {{ ref('stg_csds_service_type_history') }}
qualify row_number() over (
    partition by unique_service_request_identifier
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id::number desc, cyp102_unique_id::number desc
) = 1

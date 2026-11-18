{{ config(materialized='table') }}

-- ETOS successor key: referral, onward referral date and reason.
select *
from {{ ref('stg_csds_onward_referral_history') }}
qualify row_number() over (
    partition by unique_service_request_identifier, onward_referral_date, onward_referral_reason
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id::number desc, cyp105_unique_id::number desc
) = 1

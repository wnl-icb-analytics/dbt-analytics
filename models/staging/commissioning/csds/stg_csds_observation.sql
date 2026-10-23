{{ config(materialized='table') }}

select
    r.cyp611_unique_id
    , r.care_activity_identifier
    , r.unique_care_activity_identifier
    , r.person_weight
    , r.person_height_in_metres
    , r.person_length_in_centimetres
    , r.dmic_observation_date::date as dmic_observation_date
    , r.person_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.unique_submission_id
    , r.record_number
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp611obs') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id

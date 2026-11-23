{{ config(materialized='table') }}

with keyed as (
    select
        *
        -- A missing date is part of the key; missing person, provider or code keeps each occurrence.
        , {{ dbt_utils.generate_surrogate_key(['person_id', 'organisation_code_provider', 'social_circumstance_code', 'recorded_date',
            "iff(person_id is null or organisation_code_provider is null or social_circumstance_code is null, cyp006_unique_id::varchar, null)"]) }} as source_record_id
    from {{ ref('stg_csds_social_circumstance_history') }}
)

select
    *
    , min(reporting_period_end_date::date) over (partition by source_record_id) as first_reported_period_end_date
    , max(reporting_period_end_date::date) over (partition by source_record_id) as last_reported_period_end_date
    , count(*) over (partition by source_record_id) as accepted_source_record_count
from keyed
qualify row_number() over (
    partition by source_record_id
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id::number desc, cyp006_unique_id desc
) = 1

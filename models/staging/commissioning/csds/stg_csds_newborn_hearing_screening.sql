{{ config(materialized='table') }}

with keyed as (
    select
        *
        -- ETOS derivation key; rows missing a required field keep their own occurrence.
        , {{ dbt_utils.generate_surrogate_key(['person_id', 'organisation_code_provider', 'newborn_hearing_screening_outcome', 'audiology_service_request_date', 'audiology_procedure_date',
            "iff(person_id is null or organisation_code_provider is null, cyp603_unique_id::varchar, null)"]) }} as source_record_id
    from {{ ref('stg_csds_newborn_hearing_screening_history') }}
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
        unique_submission_id::number desc, cyp603_unique_id desc
) = 1

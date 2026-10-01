{{ config(materialized='table') }}

with keyed as (
    select
        *
        -- ETOS derivation key plus person, so conflicting identities stay separate; rows missing a required field keep their own occurrence.
        , {{ dbt_utils.generate_surrogate_key(['unique_service_request_identifier', 'person_id', 'diagnosis_scheme_code', 'diagnosis_code', 'diagnosis_date',
            "iff(unique_service_request_identifier is null or diagnosis_scheme_code is null or diagnosis_code is null, cyp606_unique_id::varchar, null)"]) }} as source_record_id
    from {{ ref('stg_csds_provisional_diagnosis_history') }}
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
        unique_submission_id::number desc, cyp606_unique_id desc
) = 1

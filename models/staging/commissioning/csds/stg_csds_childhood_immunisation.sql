{{ config(materialized='table') }}

with keyed as (
    select
        *
        -- ETOS derivation key; rows missing a required field keep their own occurrence.
        , {{ dbt_utils.generate_surrogate_key(['person_id', 'organisation_code_provider', 'childhood_immunisation_type', 'immunisation_date',
            "iff(person_id is null or organisation_code_provider is null or childhood_immunisation_type is null or immunisation_date is null, cyp502_unique_id::varchar, null)"]) }} as source_record_id
    from {{ ref('stg_csds_childhood_immunisation_history') }}
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
        unique_submission_id::number desc, cyp502_unique_id desc
) = 1

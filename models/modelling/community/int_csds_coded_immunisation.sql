with identified as (
    select
        *
        , procedure_scheme_in_use_community_care as coding_scheme_code
        -- Unknown people and incomplete clinical keys retain individual occurrences.
        , {{ dbt_utils.generate_surrogate_key([
            'person_id', 'organisation_code_provider', 'coding_scheme_code',
            'immunisation_procedure_clinical_terminology', 'immunisation_date::date',
            "iff(person_id is null or organisation_code_provider is null or coding_scheme_code is null
                or immunisation_procedure_clinical_terminology is null
                or immunisation_date is null,
                cyp501_unique_id::varchar, null)"
        ]) }} as source_record_id
    from {{ ref('stg_csds_coded_immunisation') }}
)

select
    *
    , min(reporting_period_end_date::date) over (partition by source_record_id)
        as first_reported_period_end_date
    , max(reporting_period_end_date::date) over (partition by source_record_id)
        as last_reported_period_end_date
    , count(*) over (partition by source_record_id) as accepted_source_record_count
from identified
qualify row_number() over (
    partition by source_record_id
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id desc, cyp501_unique_id desc
) = 1

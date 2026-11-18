with measurement_code as (
    select
        'observation' as record_type
        , snomed_code
        , snomed_uk_preferred_term
        , snomed_term
        , snomed_description
        , ecds_description
        , ecds_group1
        , notes
        , valid_from_date
        , source_file_name
    from {{ ref('ecds_coded_observation') }}

    union all

    select
        'scored_assessment' as record_type
        , snomed_code
        , snomed_uk_preferred_term
        , snomed_term
        , snomed_description
        , ecds_description
        , ecds_group1
        , notes
        , valid_from_date
        , source_file_name
    from {{ ref('ecds_coded_assessment_tool_type') }}
),

labelled as (
    select
        *,
        -- Releases before v3.1.0 use the older SNOMED term columns.
        coalesce(snomed_uk_preferred_term, snomed_term, snomed_description) as preferred_term
    from measurement_code
)

select
    record_type
    , snomed_code
    , preferred_term
    , case
        when record_type = 'observation' then preferred_term
        when ecds_group1 = 'NEWS2' then 'National Early Warning Score 2'
        when ecds_group1 = 'PAIN' then 'Pain'
        when snomed_code = '1239211000000103' then 'Delirium'
        when snomed_code = '763264000' then 'Frailty'
    end as measurement_category
    , ecds_description
    , ecds_group1
    , notes
    , valid_from_date as valid_from
    , source_file_name
    , try_to_timestamp_ntz(left(source_file_name, 14), 'YYYYMMDDHH24MISS') as source_file_at
from labelled
where nullif(snomed_code, '') is not null

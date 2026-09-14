-- The long-form UEC code models must retain every non-null source code. Summing
-- observation_count restores source-record grain after distinct codes are
-- collapsed within each attendance.
with expected as (
    select 'diagnosis' as observation_type, count(*) as source_records
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }}
    where code is not null

    union all

    select 'investigation', count(*)
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }}
    where code is not null

    union all

    select 'treatment', count(*)
    from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }}
    where code is not null

    union all

    select 'comorbs', count(*)
    from {{ ref('stg_sus_ecds_clinical_comorbidities') }}
    where code is not null

    union all

    select 'findings', count(*)
    from {{ ref('stg_sus_ecds_clinical_coded_findings') }}
    where code is not null
),

actual as (
    select 'diagnosis' as observation_type, sum(observation_count) as model_records
    from {{ ref('int_sus_uec_diagnosis') }}

    union all

    select observation_type, sum(observation_count)
    from {{ ref('int_sus_uec_procedure') }}
    group by observation_type
)

select
    coalesce(e.observation_type, a.observation_type) as observation_type
    , e.source_records
    , a.model_records
from expected as e
full outer join actual as a using (observation_type)
where not equal_null(e.source_records, a.model_records)

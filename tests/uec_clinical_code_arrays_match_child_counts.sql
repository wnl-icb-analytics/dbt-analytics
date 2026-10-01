-- int_sus_uec_encounter_clinical_codes must hold every coded child record: for
-- each collection the count column and both array sizes equal the number of
-- non-null codes in its staging feed, including the separate Coded Clinical
-- Findings list. Secondary diagnoses exclude source
-- position 1, matching the model, because this test reconciles what the model
-- kept against what it selected. That the excluded row is the primary diagnosis
-- carried on int_sus_uec_encounter is a separate promise, asserted by
-- uec_primary_diagnosis_is_the_first_listed: counts alone cannot check it,
-- since removing one row per attendance from either rule leaves the same
-- total.
with staging_counts as (
    select 'secondary_diagnoses' as collection, primarykey_id, count(*) as staging_codes
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }}
    where code is not null
      and snomed_id >= 2
    group by primarykey_id

    union all

    select 'investigations', primarykey_id, count(*)
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }}
    where code is not null
    group by primarykey_id

    union all

    select 'treatments', primarykey_id, count(*)
    from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }}
    where code is not null
    group by primarykey_id

    union all

    select 'comorbidities', primarykey_id, count(*)
    from {{ ref('stg_sus_ecds_clinical_comorbidities') }}
    where code is not null
    group by primarykey_id

    union all

    select 'findings', primarykey_id, count(*)
    from {{ ref('stg_sus_ecds_clinical_coded_findings') }}
    where code is not null
    group by primarykey_id
),

model_counts as (
    select
        'secondary_diagnoses' as collection
        , visit_occurrence_id
        , n_secondary_diagnoses as model_count
        , array_size(secondary_diagnosis_codes) as code_array_size
        , array_size(secondary_diagnoses) as object_array_size
    from {{ ref('int_sus_uec_encounter_clinical_codes') }}

    union all

    select 'investigations', visit_occurrence_id, n_investigations
        , array_size(investigation_codes), array_size(investigations)
    from {{ ref('int_sus_uec_encounter_clinical_codes') }}

    union all

    select 'treatments', visit_occurrence_id, n_treatments
        , array_size(treatment_codes), array_size(treatments)
    from {{ ref('int_sus_uec_encounter_clinical_codes') }}

    union all

    select 'comorbidities', visit_occurrence_id, n_comorbidities
        , array_size(comorbidity_codes), array_size(comorbidities)
    from {{ ref('int_sus_uec_encounter_clinical_codes') }}

    union all

    select 'findings', visit_occurrence_id, n_findings
        , array_size(finding_codes), array_size(findings)
    from {{ ref('int_sus_uec_encounter_clinical_codes') }}
)

select
    coalesce(m.collection, s.collection) as collection
    , coalesce(m.visit_occurrence_id, s.primarykey_id) as visit_occurrence_id
    , m.model_count
    , m.code_array_size
    , m.object_array_size
    , s.staging_codes
from model_counts as m
full outer join staging_counts as s
    on m.visit_occurrence_id = s.primarykey_id
    and m.collection = s.collection
where coalesce(m.model_count, 0) <> coalesce(s.staging_codes, 0)
   or coalesce(m.code_array_size, 0) <> coalesce(s.staging_codes, 0)
   or coalesce(m.object_array_size, 0) <> coalesce(s.staging_codes, 0)

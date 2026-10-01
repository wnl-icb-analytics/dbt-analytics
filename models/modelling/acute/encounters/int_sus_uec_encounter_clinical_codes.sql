{{ config(materialized = 'table') }}

/*
ECDS clinical codes per attendance, as ordered arrays.

One row per attendance that has at least one coded diagnosis, investigation,
treatment, comorbidity or Coded Clinical Finding. Each feed has a plain code
array for filtering and an array of objects for reading. Both retain source
sequence order and nothing is truncated. Objects carry seq, code and desc,
plus the recorded timestamp (at) for investigations and treatments and the
SNOMED qualifier (confirmed/suspected) for diagnoses. ECDS holds no
investigation results. For one-row-per-code analysis use int_sus_uec_diagnosis
(diagnoses) and int_sus_uec_procedure (investigations, treatments,
comorbidities and Coded Clinical Findings via observation_type).
*/

with snomed as (
    select snomed_code, preferred_term
    from {{ ref('stg_dictionary_snomed_concept') }}
),

diagnoses as (
    select
        c.primarykey_id
        , array_agg(c.code) within group (order by c.snomed_id) as secondary_diagnosis_codes
        , array_agg(object_construct(
                'seq', c.snomed_id,
                'code', c.code,
                'desc', coalesce(d.snomed_uk_preferred_term, s.preferred_term),
                'qualifier_code', c.qualifier,
                'qualifier', q.preferred_term
            )) within group (order by c.snomed_id) as secondary_diagnoses
        , count(*) as n_secondary_diagnoses
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }} as c
    left join {{ ref('stg_dictionary_ecds_diagnosis') }} as d on c.code = d.snomed_code
    left join snomed as s on c.code = s.snomed_code
    left join snomed as q on c.qualifier = q.snomed_code
    where c.code is not null
      and c.snomed_id >= 2  -- position 1 is the primary diagnosis, carried on the encounter
    group by c.primarykey_id
),

investigations as (
    select
        c.primarykey_id
        , array_agg(c.code) within group (order by c.snomed_id) as investigation_codes
        , array_agg(object_construct(
                'seq', c.snomed_id,
                'code', c.code,
                'desc', coalesce(d.snomed_uk_preferred_term, s.preferred_term),
                'at', iff(c.date is null, null, timestamp_ntz_from_parts(c.date, coalesce(c.time, '00:00'::time)))
            )) within group (order by c.snomed_id) as investigations
        , count(*) as n_investigations
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }} as c
    left join {{ ref('stg_dictionary_ecds_investigation') }} as d on c.code = d.snomed_code
    left join snomed as s on c.code = s.snomed_code
    where c.code is not null
    group by c.primarykey_id
),

treatments as (
    select
        c.primarykey_id
        , array_agg(c.code) within group (order by c.snomed_id) as treatment_codes
        , array_agg(object_construct(
                'seq', c.snomed_id,
                'code', c.code,
                'desc', coalesce(d.snomed_uk_preferred_term, s.preferred_term),
                'at', iff(c.date is null, null, timestamp_ntz_from_parts(c.date, coalesce(c.time, '00:00'::time)))
            )) within group (order by c.snomed_id) as treatments
        , count(*) as n_treatments
    from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }} as c
    left join {{ ref('stg_dictionary_ecds_treatment') }} as d on c.code = d.snomed_code
    left join snomed as s on c.code = s.snomed_code
    where c.code is not null
    group by c.primarykey_id
),

comorbidities as (
    select
        c.primarykey_id
        , array_agg(c.code) within group (order by c.comorbidities_id) as comorbidity_codes
        , array_agg(object_construct(
                'seq', c.comorbidities_id,
                'code', c.code,
                'desc', coalesce(d.snomed_uk_preferred_term, s.preferred_term)
            )) within group (order by c.comorbidities_id) as comorbidities
        , count(*) as n_comorbidities
    from {{ ref('stg_sus_ecds_clinical_comorbidities') }} as c
    left join {{ ref('stg_dictionary_ecds_comorbidity') }} as d on c.code = d.snomed_code
    left join snomed as s on c.code = s.snomed_code
    where c.code is not null
    group by c.primarykey_id
),

findings as (
    select
        c.primarykey_id
        , array_agg(c.code) within group (order by c.coded_findings_id) as finding_codes
        , array_agg(object_construct_keep_null(
                'seq', c.coded_findings_id,
                'code', c.code,
                'desc', coalesce(d.snomed_uk_preferred_term, s.preferred_term)
            )) within group (order by c.coded_findings_id) as findings
        , count(*) as n_findings
    from {{ ref('stg_sus_ecds_clinical_coded_findings') }} as c
    left join {{ ref('stg_dictionary_ecds_codedfinding') }} as d
        on c.code = d.snomed_code
    left join snomed as s on c.code = s.snomed_code
    where c.code is not null
    group by c.primarykey_id
),

attendances as (
    select primarykey_id from diagnoses
    union
    select primarykey_id from investigations
    union
    select primarykey_id from treatments
    union
    select primarykey_id from comorbidities
    union
    select primarykey_id from findings
)

select
    a.primarykey_id as visit_occurrence_id
    , d.secondary_diagnosis_codes
    , d.secondary_diagnoses
    , coalesce(d.n_secondary_diagnoses, 0) as n_secondary_diagnoses
    , i.investigation_codes
    , i.investigations
    , coalesce(i.n_investigations, 0) as n_investigations
    , t.treatment_codes
    , t.treatments
    , coalesce(t.n_treatments, 0) as n_treatments
    , c.comorbidity_codes
    , c.comorbidities
    , coalesce(c.n_comorbidities, 0) as n_comorbidities
    , f.finding_codes
    , f.findings
    , coalesce(f.n_findings, 0) as n_findings
from attendances as a
left join diagnoses as d on a.primarykey_id = d.primarykey_id
left join investigations as i on a.primarykey_id = i.primarykey_id
left join treatments as t on a.primarykey_id = t.primarykey_id
left join comorbidities as c on a.primarykey_id = c.primarykey_id
left join findings as f on a.primarykey_id = f.primarykey_id

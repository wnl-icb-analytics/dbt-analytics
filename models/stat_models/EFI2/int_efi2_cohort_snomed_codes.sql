{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}


select distinct
    pd.person_id,
    cd.deficit,
    cd.other_instructions,
    cd.snomedct_conceptid as snomed_code,
    cd.code_description,
    cd.time_constraint_years,
    cd.age_limit,
    obs.result_value,
    obs.result_unit_display as result_value_unit,
    obs.clinical_effective_date,
    datediff(year, pd.date_of_birth, date(obs.clinical_effective_date)) as age_at_event,
    datediff(year, pd.date_of_birth, date(pd.end_date)) as age_at_end,
    pd.date_of_birth,
    pd.date_of_death,
    pd.gender,
    pd.end_date

from {{ ref('int_efi2_patient_list') }} pd
left join {{ ref('stg_olids_observation') }} obs
    on obs.person_id = pd.person_id
inner join {{ ref('stg_common_efi2_codelists') }} cd
    on cd.snomedct_conceptid::varchar = obs.mapped_concept_code::varchar

where
    obs.clinical_effective_date <= pd.end_date
    -- Time constraint: observation must be within time_constraint_years of end_date
    and (
        dateadd(year, cd.time_constraint_years, obs.clinical_effective_date) > pd.end_date
        or cd.time_constraint_years is null
    )
    -- Age constraint
    and (
        datediff(year, pd.date_of_birth, date(obs.clinical_effective_date)) >= cd.age_limit
        or cd.age_limit is null
    )

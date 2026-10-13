-- Latest DM_COD/DMRES_COD determines Type 1 HRC membership, one row per person.
with latest_diabetes as (
    select *
    from {{ ref('int_diabetes_diagnoses_all') }}
    where (is_general_diabetes_code or is_diabetes_resolved_code)
        and clinical_effective_date <= current_date()
    -- EMIS does not specify same-day ordering. Recorded date, then ID, break ties.
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, date_recorded desc nulls last, id desc
    ) = 1
),
source_type1 as (
    {{ get_ltc_lcs_observations('0dd187c0-35b5-10be-2837-4a1ad771a6e8', match_paths='source') }}
),
mapped_type1 as (
    {{ get_ltc_lcs_observations('0dd187c0-35b5-10be-2837-4a1ad771a6e8', match_paths='mapped') }}
),
type1_records as (
    select observation_id from source_type1
    union
    select m.observation_id
    from mapped_type1 m
    inner join {{ ref('stg_olids_observation') }} o on m.observation_id = o.id
    inner join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.observation_source_concept_id = ecm.source_concept_id
    -- EMIS-local 1000006 descriptions must qualify through the source path only.
    where substr(ecm.source_code, -10, 7) <> '1000006'
)
select
    d.person_id,
    'Type 1 Diabetes' as condition,
    'HRC' as risk_group
from latest_diabetes d
inner join type1_records t on d.id = t.observation_id

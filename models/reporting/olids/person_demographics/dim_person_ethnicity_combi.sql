with bridge_ethnicity as (
    select distinct 
        snomed_code::varchar as snomed_code, 
        subcategory::varchar as subcategory, 
        bk_ethnicity_code::varchar as bk_ethnicity_code
    from {{ref('ethnicity_snomed_ONS_NHS')}}
),

gp_ethnicity as (
    select distinct
        pp.sk_patient_id::varchar as sk_patient_id,
        dat.latest_ethnicity_date::date as code_date,
        'EMIS'::varchar as source_encounter,
        dat.snomed_code::varchar as source_code,
        dict.subcategory::varchar as ethnicity,
    from {{ref('dim_person_ethnicity')}} dat
    left join  {{ref('dim_person_pseudo')}} pp ON dat.person_id = pp.person_id
    left join {{ref('ethnicity_snomed_ONS_NHS')}} dict on dict.snomed_code = dat.snomed_code
),

event_ethnicity as (
    select 
        de.sk_patient_id::varchar as sk_patient_id,
        de.code_date::date as code_date,
        de.visit_occurrence_type::varchar as source_encounter,
        de.ethnicity_at_event::varchar as source_code,
        dict.subcategory::varchar as ethnicity
    from {{ ref('int_encounter_demographics') }} de
    left join {{ref('ethnicity_snomed_ONS_NHS')}} dict on dict.bk_ethnicity_code = de.ethnicity_at_event
    where ethnicity_at_event is not null and ethnicity_at_event not IN ('Z', '99')
),

combined_ethnicity as (
    select * from gp_ethnicity
    union
    select * from event_ethnicity
)

select
    ce.sk_patient_id,
    pp.person_id,
    code_date,
    source_encounter,
    source_code,
    ethnicity
from combined_ethnicity ce
left join {{ref('dim_person_pseudo')}} pp ON ce.sk_patient_id = pp.sk_patient_id
where UPPER(TRIM(ethnicity)) not in ('RECORDED NOT KNOWN','NOT RECORDED','NOT STATED','REFUSED')
qualify row_number() over (partition by ce.sk_patient_id order by code_date desc) = 1
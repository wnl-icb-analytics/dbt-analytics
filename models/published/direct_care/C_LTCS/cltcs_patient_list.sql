with in_scope_practice_list as (
    select  practice_code, area_code, area_name
    from {{ ref('cltcs_organisations_in_scope')}}
), 

adult_population as (
    select cast(sk_patient_id as varchar) as patient_id
        , neighbourhood_code as area_code
        , person_id as olids_id
        , practice_code
        , meets_original_criteria
        , meets_adults_with_complexity
    from {{ ref('cltcs_adult_population') }}
),

registrant_list as (
    select patient_id
        , area_code
    from adult_population
    -- old areas original criteria or any area new criteria
    where (meets_original_criteria = TRUE or meets_adults_with_complexity = TRUE)
    and practice_code in (select distinct practice_code from in_scope_practice_list)
),

-- open cases stay listed, at their status log area, until closed, removed or rejected
open_case_list as (
    select cast(patient_id as varchar) as patient_id
        , area_code
    from {{ ref('cltcs_status_log_open_cases') }}
),

-- olids_id for open cases no longer in the adult population; one person_id per patient
pseudo_lookup as (
    select oc.patient_id
        , pp.person_id as olids_id
    from open_case_list oc
    inner join {{ ref('dim_person_pseudo') }} pp on cast(pp.sk_patient_id as varchar) = oc.patient_id
    qualify row_number() over (partition by oc.patient_id order by pp.person_id) = 1
),

patient_spine as (
    select patient_id from registrant_list
    union
    select patient_id from open_case_list
)

select s.patient_id
    , coalesce(oc.area_code, rl.area_code) as area_code
    , coalesce(ap.olids_id, pl.olids_id) as olids_id
    , case when oc.patient_id is not null then 'shortlist' else 'registrant' end as source
    -- current criteria; null when the patient is no longer in cltcs_adult_population
    , ap.meets_original_criteria
    , ap.meets_adults_with_complexity
    , case when oc.patient_id is not null then 1 else 0 end as shortlist_flag
    , case when rl.patient_id is not null then 1 else 0 end as reg_flag
from patient_spine s
left join registrant_list rl on rl.patient_id = s.patient_id
left join open_case_list oc on oc.patient_id = s.patient_id
left join adult_population ap on ap.patient_id = s.patient_id
left join pseudo_lookup pl on pl.patient_id = s.patient_id
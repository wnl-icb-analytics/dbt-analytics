with in_scope_practice_list as (
    select  practice_code, area_code, area_name
    from {{ ref('cltcs_organisations_in_scope')}}
), 

registrant_list as (
    select cast(sk_patient_id as varchar) as patient_id
        , neighbourhood_code as area_code
        , person_id as olids_id
        , 'registrant' as source
    from {{ ref('cltcs_adult_population') }}
    where fragmented_sk_patient_id_flag = 0 and fragmented_person_id_flag = 0 -- exclude fragmented patients for now
    and practice_code in (select distinct practice_code from in_scope_practice_list)
),

shortlisted_list as (
    select sl.patient_id, sl.area_code, pp.person_id as olids_id, 'shortlist' as source
    from {{ ref('cltcs_status_log_most_recent') }} sl
    left join {{ref('dim_person_pseudo')}} pp on pp.sk_patient_id = sl.patient_id
    where action not in ('Reject', 'Case closed', 'Removed from shortlist') 
),

emis_list as (
    select ed.patient_id, ed.area_code, pp.person_id as olids_id,'emis' as source
    from {{ ref('cltcs_emis_data') }} ed
    left join {{ref('dim_person_pseudo')}} pp on pp.sk_patient_id = ed.patient_id
),

complete_list as (
    select * from registrant_list
    union all
    select * from shortlisted_list
    union all
    select * from emis_list
),

expanded_registrant_list as (
    select patient_id
        , area_code
        , olids_id
        , source
    from complete_list c
    qualify row_number() over (
        partition by patient_id
        order by case
            when source = 'emis' then 0
            when source = 'shortlist' then 1
            when source = 'registrant' then 2
            else 3
        end
    ) = 1)

select erl.patient_id
    , erl.area_code
    , erl.olids_id
    , erl.source
    , case when exists (select 1 from shortlisted_list sl where sl.patient_id = erl.patient_id) then 1 else 0 end as shortlist_flag
    , case when exists (select 1 from emis_list el where el.patient_id = erl.patient_id) then 1 else 0 end as emis_flag
    , case when exists (select 1 from registrant_list rl where rl.patient_id = erl.patient_id) then 1 else 0 end as registrant_flag
from expanded_registrant_list erl
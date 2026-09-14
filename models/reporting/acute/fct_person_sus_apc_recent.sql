with 
base_encounters_raw as (
    select *
    from {{ ref('int_sus_apc_imputed_spells') }}
    where (start_date between dateadd(month, -12, current_date()) and current_date()
        or end_date between dateadd(month, -12, current_date()) and current_date())
    and sk_patient_id is not null and sk_patient_id != '1'
    and spell_admission_method not in ('2C', '82', '31') -- birth of a baby
),

base_population as ( -- all people in PDS (includes dead and not, registered and not)
    select distinct sk_patient_id
    from {{ref('dim_person_demographics_basic')}}
    where sk_patient_id is not null and sk_patient_id != '1'
),  

apc_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when start_date between dateadd(month, -3, current_date()) and current_date() 
                then visit_occurrence_id end) as apc_3mo
        , count(distinct case when start_date between dateadd(month, -1, current_date()) and current_date() 
                then visit_occurrence_id end) as apc_1mo
        , count(distinct case when start_date between dateadd(month, -12, current_date()) and current_date()
                then visit_occurrence_id end) as apc_12mo
        , count(distinct case when left(spell_admission_method, 1) = '2' -- Non-elective - emergency
                and start_date between dateadd(month, -12, current_date()) and current_date()
                then visit_occurrence_id end) as apc_nel_12mo
        , sum(iff(dq_start_date, 1, 0)) as dq_start_date_count
        , sum(iff(dq_end_date, 1, 0)) as dq_end_date_count
        , sum(iff(dq_duration, 1, 0)) as dq_duration_count
    from base_encounters_raw
    group by 
        sk_patient_id
),
ambulatory_sensitive_nel_encounters as ( -- ambulatory sensitive NEL admissions
    select sk_patient_id 
        , count(distinct visit_occurrence_id) as acs_nel_12mo
    from {{ref('int_activity_ambulatory_sensitive_nel') }} 
    where start_date between dateadd(month, -12, current_date()) and current_date()
    and sk_patient_id is not null and sk_patient_id != '1'
    group by sk_patient_id
),
population_spine as (
    select sk_patient_id from base_population
    union 
    select sk_patient_id from apc_encounter_summary
    union 
    select sk_patient_id from ambulatory_sensitive_nel_encounters
)

SELECT
    ps.sk_patient_id
    , greatest(0, zeroifnull(a.apc_3mo)) as apc_3mo
    , greatest(0, zeroifnull(a.apc_1mo)) as apc_1mo
    , greatest(0, zeroifnull(a.apc_12mo)) as apc_12mo
    , greatest(0, zeroifnull(m.apc_los_12mo)) as apc_los_12mo
    , greatest(0, zeroifnull(a.apc_nel_12mo)) as apc_nel_12mo
    , greatest(0, zeroifnull(as_nel.acs_nel_12mo)) as acs_nel_12mo
    , greatest(0, zeroifnull(a.dq_duration_count)) as dq_duration_count
    , greatest(0, zeroifnull(a.dq_start_date_count)) as dq_start_date_count
    , greatest(0, zeroifnull(a.dq_end_date_count)) as dq_end_date_count
from population_spine ps
left join apc_encounter_summary as a
    on ps.sk_patient_id = a.sk_patient_id
left join {{ ref('int_sus_apc_merged_spells') }} as m
    on ps.sk_patient_id = m.sk_patient_id
left join ambulatory_sensitive_nel_encounters as_nel
    on ps.sk_patient_id = as_nel.sk_patient_id
where ps.sk_patient_id is not null and ps.sk_patient_id != '1'
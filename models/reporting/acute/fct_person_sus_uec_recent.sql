with base_encounters as ( -- emergency attendances
    select *
    from {{ ref('int_sus_uec_encounter') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
    and sk_patient_id is not null and sk_patient_id != '1'
),  

base_population as ( -- all GP registered patients
    select distinct sk_patient_id
    from {{ref('dim_person_demographics_basic')}}
    where sk_patient_id is not null and sk_patient_id != '1'
),  

lower_respiratory_encounters as ( -- lower respiratory encounters
    select *
    from {{ref('int_activity_chronic_lower_respiratory') }}
    where start_date between dateadd(month, -12, current_date()) and current_date()
    and sk_patient_id is not null and sk_patient_id != '1'
),  

emergency_admissions as ( -- emergency admissions for respiratory conditions
    select ip.sk_patient_id 
        , count(distinct ip.visit_occurrence_id) as ae_respiratory_admission_12mo
    from {{ ref('int_sus_apc_encounter') }} as ip
    inner join lower_respiratory_encounters as lre on lre.visit_occurrence_id = ip.visit_occurrence_id
    where ip.start_date between dateadd(month, -12, current_date()) and current_date()
    and ip.sk_patient_id is not null and ip.sk_patient_id != '1'
    and left(ip.spell_admission_method, 1) = '2' -- Non-elective - emergency
    group by 
        ip.sk_patient_id
),

population_spine as (
    select sk_patient_id from base_population
    union 
    select sk_patient_id from base_encounters
    union 
    select sk_patient_id from emergency_admissions
),

ae_encounter_summary as(
    select
        be.sk_patient_id
        , count(distinct case when is_injury_related = FALSE  -- Attended - not injury -- TO DO: stratify by department type
                then be.visit_occurrence_id end) as ae_ill_12mo
        , count(distinct case when is_injury_related = FALSE -- Attended - not injury
                and be.start_date between dateadd(month, -3, current_date()) and current_date() 
                then be.visit_occurrence_id end) as ae_ill_3mo
        , count(distinct case when is_injury_related = FALSE  -- Attended - not injury
                and be.start_date between dateadd(month, -1, current_date()) and current_date() 
                then be.visit_occurrence_id end) as ae_ill_1mo
        , count(distinct be.visit_occurrence_id) as ae_tot_12mo -- all attendances
        , count(distinct case when be.start_date between dateadd(month, -3, current_date()) and current_date() 
                then be.visit_occurrence_id end) as ae_tot_3mo
        , count(distinct case when is_injury_related = TRUE-- all injuries
                then be.visit_occurrence_id end) as ae_inj_12mo
        , count(distinct case when clr.lower_respiratory_encounter = true -- respiratory
                then be.visit_occurrence_id end) as ae_respiratory_attendance_12mo
        , count(distinct case when pod = 'AE-T1' -- Type 1 A&E
                then be.visit_occurrence_id end) as ae_t1_12mo
    from base_encounters be
    left join lower_respiratory_encounters clr 
    on be.visit_occurrence_id = clr.visit_occurrence_id
    group by 
        be.sk_patient_id
)

SELECT
    ps.sk_patient_id
    , zeroifnull(a.ae_ill_12mo) as ae_ill_12mo
    , zeroifnull(a.ae_ill_3mo) as ae_ill_3mo
    , zeroifnull(a.ae_ill_1mo) as ae_ill_1mo
    , zeroifnull(a.ae_tot_12mo) as ae_tot_12mo
    , zeroifnull(a.ae_tot_3mo) as ae_tot_3mo
    , zeroifnull(a.ae_inj_12mo) as ae_inj_12mo
    , zeroifnull(a.ae_t1_12mo) as ae_t1_12mo
    , zeroifnull(a.ae_respiratory_attendance_12mo) as ae_lower_respiratory_attendance_12mo -- 500k million attendance >= admission
    , zeroifnull(ea.ae_respiratory_admission_12mo) as ae_lower_respiratory_admission_12mo --  15k admission > attendance
from population_spine ps
left join ae_encounter_summary as a
    on ps.sk_patient_id = a.sk_patient_id
left join emergency_admissions ea
    on ps.sk_patient_id = ea.sk_patient_id

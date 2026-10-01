{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}

/* EVALUATION SCRIPT USING PROPENSITY MATCHED GROUPS WITH JESS AMENDMENTS 2.6.2026 person level member costs
3.8.26 adding in OP, APC Elective and GP activity
4.8.26 also testing new propensity matching process with caliper and control re-use up to 3 max
*/

with date_range as (-- Generate days for 2 years
    select 
         dateadd('day', seq4() - 700, current_date) as date
    from 
        table(generator(rowcount => 700))
    where 
        date <= current_date()
)
--memberspine derived from matched group of patients rather than whole eligible population - select the latest PROPENSITY MATCH FILE
, member_spine as (
     select
        mp.patient_id,
        mp.hex_id,
        -- mp.local_authority,
        -- mp.first_file_date as eligibility_date,
        -- mp.onboarded_date,
        -- mp.death_date,
        -- case when mp.group_value = 1 then 'Onboarded 60+ days' else 'Not onboarded' end as myria_status
    --CHANGES AFTER MATCHING WITH CONTROL RE_USE - auagust 2026 - Jess
    max(mp.local_authority) as local_authority,
    max(mp.first_file_date) as eligibility_date,
    max(mp.onboarded_date) as onboarded_date,
    max(mp.death_date) as death_date,
    case when max(mp.group_value) = 1 then 'Onboarded 60+ days' else 'Not onboarded' end as myria_status,
    count(*) as match_weight -- 1 for treated; reuse count for controls
    from {{ ref('stg_myria_matched_patients') }} mp
    --derived from notebook with INCLUDED_DISCHARGED = TRUE
  --from STAGING.MYRIA.STG_MYRIA_MATCHED_PATIENTS mp
     where file_date = (select max(file_date) from {{ ref('stg_myria_matched_patients') }})
   --WHERE file_date = (select max(first_file_date) from STAGING.MYRIA.STG_MYRIA_MATCHED_PATIENTS) -- DON'T Need this for testing
   group by all
)
 /*Build activity data (A&E): ae_encounter_summary as Counts:encounters, cost,duration
limit to eligible group 66K rows by inner join with member spine. Added date range limits to reduce rows from 15 million!
*/
, ae_encounter_summary as (
    select
        uec.sk_patient_id
        ,m.hex_id
        , start_date as activity_date
        , end_date as end_date
        , date(start_date) as activity_date_range
        , 'A&E' as activity_type
        , pod as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from {{ ref("obt_encounter_uec") }} uec
    --from REPORTING.COMMISSIonING_REPORTING.OBT_ENCOUNTER_UEC uec
        inner join member_spine m on uec.sk_patient_id = m.patient_id
        inner join date_range d on d.date >= uec.start_date  and d.date <= coalesce(uec.end_date, current_date)
    where 
        sk_patient_id is not null and sk_patient_id != '1' 
        and start_date <= dateadd(dd, -7, current_date)
   group by all     
)
 /*Build activity data (OP): op_encounter_summary as Counts:encounters, cost, expected duration (in minutes). Start date is also end date. Attended activity only 
limit to eligible group 66K rows by inner join with member spine. Added date range limits to reduce rows from 15 million!
*/
, op_encounter_summary as (
    select
        op.sk_patient_id
        ,m.hex_id
        , start_date as activity_date
        , start_date as end_date
        , date(start_date) as activity_date_range
        , 'OP' as activity_type
        , pod as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(expected_duration) as duration
    from {{ ref("obt_encounter_op") }} op
    --from REPORTING.COMMISSIonING_REPORTING.OBT_ENCOUNTER_OP op
        inner join member_spine m on op.sk_patient_id = m.patient_id
        inner join date_range d on d.date >= op.start_date  and d.date <= coalesce(op.start_date, current_date)
    where 
        sk_patient_id is not null and sk_patient_id != '1' 
        and start_date <= dateadd(dd, -7, current_date)
   group by all   
)
/* Build Inpatient activity: apc_encounter_summary (NEL)
Expands each stay into daily rows via date_range eg: Admission = 01-Jan to 03-Jan becomes 3 rows 01-Jan, 02-Jan, 03-Jan
Calculates: encounters (on admission day), cost per day, duration (length of stay)
limit by eligible patient activity by inner join with member spine.
testing alternative which includes: completed admissions if they overlap date_range, 
Ongoing admissions up to current_date and Non-overlapping admissions dropped (no NULL rows)*/
, apc_encounter_summary as(
   select
        apc.sk_patient_id
        ,m.hex_id
        , apc.start_date as activity_date
        , apc.end_date
        , d.date as activity_date_range
        , 'Inpatient' as activity_type
        , case when apc.spec_comm_flag = 'Y' then 'spec_comm' else apc.admission_method_group end as activity_subtype
        , sum(case when d.date = apc.start_date then 1 end) as encounters
        -- spreads costs against all days in spell, if open spell, all days except last 7
        , sum(cost / datediff(day, apc.start_date, coalesce(dateadd(dd, 1, apc.end_date), dateadd(dd, -6, current_date))) ) as cost 
        , sum(case when d.date = apc.start_date then 0 else 1 end) as duration 
        -- each row is 1 day. 0 day los would be 0, else 1. so 3 rows would give 2 day los - which is correct for admitted on first day and discharged 2 days later.
    from {{ ref("obt_encounter_apc") }} apc
    --from REPORTING.COMMISSIonING_REPORTING.obt_encounter_apc as apc
    inner join member_spine m on apc.sk_patient_id = m.patient_id
    -- join all days during the spell that are included in the activity date range
    --left join date_range as d on d.date <= coalesce(e.end_date, current_date) and d.date >= e.start_date
    --testing alternative which includes: completed admissions if they overlap date_range, Ongoing admissions up to current_date and Non-overlapping admissions dropped (no NULL rows)
    inner join date_range d on d.date >= apc.start_date  and d.date <= coalesce(apc.end_date, current_date)
    where 
        apc.sk_patient_id is not null and apc.sk_patient_id != '1'
        and start_date <= dateadd(dd, -7, current_date)
    group by all
)
/*
Build GP activity: gp_encounter_summary - ATTENDED ACTIVITY only
Expands each stay into daily rows via date_range eg: Appointment = 01-Jan to 03-Jan becomes 3 rows 01-Jan, 02-Jan, 03-Jan
Calculates: encounters (on apointment day), cost per day, duration (length of stay)
limit by eligible patient activity by inner join with member spine.
includes all kinds of clinical activity including triage  - keep all for now
*/
,gp_encounter_summary as (
 select
        TO_VARCHAR(p.sk_patient_id) as sk_patient_id
       ,m.hex_id
        , date(start_date) as activity_date
        , date(start_date) as end_date
        , date(start_date) as activity_date_range
        , 'GP' as activity_type
        , slot_category as activity_subtype
        , count(*) as encounters
        , sum(appointment_cost_gbp_nominal) as cost
        , sum(duration_minutes) as duration
    --FROM MODELLING.OLIDS_APPOINTMENTS.INT_APPOINTMENT_GP_CLINICAL gp
    from {{ ref("int_appointment_gp_clinical") }} gp
    --inner join REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS p using (person_id)
    inner join {{ ref("dim_person_demographics") }} p using (person_id)
   inner join member_spine m on TO_VARCHAR(p.sk_patient_id) = m.patient_id
    inner join date_range d on d.date >= date(gp.start_date)  and d.date <= coalesce(date(gp.start_date), current_date)
    where 
        p.sk_patient_id is not null and p.sk_patient_id != '1' 
        and start_date <= dateadd(dd, -7, current_date) and is_attended
    group by all
)
, combined as (
    select * from ae_encounter_summary
    union all
    select * from op_encounter_summary
    union all
    select * from apc_encounter_summary
    union all
    select * from gp_encounter_summary
)
/*daily aggregation of healthcare activity per patient. converts raw events into 
ip_nel_emergency_encounters = number of emergency admissions
ip_nel_emergency_cost = cost of those admissions
ip_nel_emergency_duration = bed days (length of stay)
August evaluation - add in Elective Costs
*/
, fct_person_activity_by_day as (
    select 
        sk_patient_id
        , hex_id
        , activity_date_range
        --GP ()
        , sum(case when activity_type = 'GP' then encounters else 0 end) as gp_encounters
        , sum(case when activity_type = 'GP' then cost else 0 end) as gp_cost
        -- A&E ()
        , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
        , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
        -- OP ()
        , sum(case when activity_type = 'OP' then encounters else 0 end) as op_encounters
        , sum(case when activity_type = 'OP' then cost else 0 end) as op_cost
        -- Inpatient NEL
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then encounters else 0 end) as ip_nel_emergency_encounters
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then cost else 0 end) as ip_nel_emergency_cost
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then duration else 0 end) as ip_nel_emergency_duration
        -- Inpatient Elective
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then encounters else 0 end) as ip_elective_encounters
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then cost else 0 end) as ip_elective_cost
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then duration else 0 end) as ip_elective_duration
    from combined
    group by all
    order by 1
)
/*This creates a daily time series per patient, even if they had no activity. Build a complete date spine, then attach costs/activity
Enables longitudinal analysis: Track cost over time, Align patients around intervention date, Compare pre vs post
Because of the date spine + left join: Patients with no activity still appear. Their cost = 0, avoids bias when calculating averages.
Revised calculation person level n~1200
 */
, member_annual_costs as (
    select
        m.hex_id ,
        m.local_authority,
        m.myria_status,
        m.match_weight,
        case when d.date <= m.eligibility_date then 'Pre-intervention' else 'Post-intervention' end as activity_status,
        count(d.date) as n_days,
        -- cost
        round (sum (ifnull(c.ip_nel_emergency_cost,0)), 10) as ip_nel_emergency_cost,
        round (sum (ifnull(c.ip_elective_cost,0)), 10) as ip_elective_cost,
        round (sum (ifnull(c.ae_cost,0)), 10) as ae_cost,
        round (sum (ifnull(c.op_cost,0)), 10) as op_cost,
        round (sum (ifnull(c.gp_cost,0)), 10) as gp_cost,
        -- encounters
        round (sum (ifnull(c.ip_nel_emergency_encounters,0)), 10) as ip_nel_emergency_encounters,
        round (sum (ifnull(c.ip_elective_encounters,0)), 10) as ip_elective_encounters,
        round (sum (ifnull(c.ae_encounters,0)), 10) as ae_encounters,
        round (sum (ifnull(c.op_encounters,0)), 10) as op_encounters,
        round (sum (ifnull(c.gp_encounters,0)), 10) as gp_encounters,
        -- bed days
        round (sum (ifnull(c.ip_nel_emergency_duration,0)), 10) as ip_nel_emergency_duration,
        round (sum (ifnull(c.ip_elective_duration,0)), 10) as ip_elective_duration
    from member_spine as m
    left join date_range as d -- date spine between 1 year before eligibility date and date of death/current date - 7 days (to allow for incomplete data)
        on d.date between dateadd(dd, -365, eligibility_date) 
            and case when m.death_date < dateadd(dd, -7, current_date()) then m.death_date else dateadd(dd, -7, current_date()) end
    left join fct_person_activity_by_day as c 
        on m.hex_id = c.hex_id
        and d.date = c.activity_date_range
     group by all
)
/*aggregates the patient-level data into annualised cohort-level cost and activity metrics, grouped by:
myria_status = cohort/group (e.g. treatment vs control),activity_status = pre vs post intervention 
So each row = one cohort × pre/post period
--adding in Jess's changes frpm 3rd August 2026 to account for control reuse. **MAKE DBT MACRO for weighted avg,sd,se
*/

select
    myria_status,
    activity_status,
    --count(*) as patients,
    count(*) as n_unique_patients,
    sum(match_weight) as n_weighted, -- controls: equals the treated N
    pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) as n_eff, -- Kish effective N
    -- NEL EMERGENCY STATS------------------------------------------------
--ENCOUNTERS
    -- weighted mean of the annualised rate
    sum(match_weight * (ip_nel_emergency_encounters / (n_days/365.0))) / sum(match_weight)
    as avg_ip_nel_emergency_encounters,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ip_nel_emergency_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_nel_emergency_encounters/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ip_nel_emergency_encounters,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ip_nel_emergency_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_nel_emergency_encounters/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ip_nel_emergency_encounters,
--COST
    -- weighted mean of the annualised rate
    sum(match_weight * (ip_nel_emergency_cost / (n_days/365.0))) / sum(match_weight)
    as avg_ip_nel_emergency_cost,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ip_nel_emergency_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_nel_emergency_cost/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ip_nel_emergency_cost,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ip_nel_emergency_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_nel_emergency_cost/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ip_nel_emergency_cost,
--DURATION
    -- weighted mean of the annualised rate
    sum(match_weight * (ip_nel_emergency_duration / (n_days/365.0))) / sum(match_weight)
    as avg_ip_nel_emergency_duration,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ip_nel_emergency_duration/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_nel_emergency_duration/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ip_nel_emergency_duration,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ip_nel_emergency_duration/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_nel_emergency_duration/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ip_nel_emergency_duration,
-- IP ELECTIVE STATS-----------------------------------------------
--ENCOUNTERS
    -- weighted mean of the annualised rate
    sum(match_weight * (ip_elective_encounters / (n_days/365.0))) / sum(match_weight)
    as avg_ip_elective_encounters,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ip_elective_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_elective_encounters/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ip_elective_encounters,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ip_elective_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_elective_encounters/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ip_elective_encounters,
--COST
    -- weighted mean of the annualised rate
    sum(match_weight * (ip_elective_cost / (n_days/365.0))) / sum(match_weight)
    as avg_ip_elective_cost,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ip_elective_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_elective_cost/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ip_elective_cost,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ip_elective_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_elective_cost/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ip_elective_cost,
--DURATION
    -- weighted mean of the annualised rate
    sum(match_weight * (ip_elective_duration / (n_days/365.0))) / sum(match_weight)
    as avg_ip_elective_duration,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ip_elective_duration/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_elective_duration/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ip_elective_duration,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ip_elective_duration/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ip_elective_duration/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ip_elective_duration,
-----AE STATS
--ENCOUNTERS
    -- weighted mean of the annualised rate
    sum(match_weight * (ae_encounters / (n_days/365.0))) / sum(match_weight)
    as avg_ae_encounters,
    -- weighted SD (paeulation form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ae_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ae_encounters/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ae_encounters,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ae_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ae_encounters/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ae_encounters,
--COST
    -- weighted mean of the annualised rate
    sum(match_weight * (ae_cost / (n_days/365.0))) / sum(match_weight)
    as avg_ae_cost,
    -- weighted SD (paeulation form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(ae_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ae_cost/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_ae_cost,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(ae_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (ae_cost/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_ae_cost,
--- OP STATS
--ENCOUNTERS
    -- weighted mean of the annualised rate
    sum(match_weight * (op_encounters / (n_days/365.0))) / sum(match_weight)
    as avg_op_encounters,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(op_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (op_encounters/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_op_encounters,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(op_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (op_encounters/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_op_encounters,
--COST
    -- weighted mean of the annualised rate
    sum(match_weight * (op_cost / (n_days/365.0))) / sum(match_weight)
    as avg_op_cost,
    -- weighted SD (population form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(op_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (op_cost/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_op_cost,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(op_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (op_cost/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_op_cost,
--- GP STATS
 --ENCOUNTERS
    -- weighted mean of the annualised rate
    sum(match_weight * (gp_encounters / (n_days/365.0))) / sum(match_weight)
    as avg_gp_encounters,
    -- weighted SD (pgpulation form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(gp_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (gp_encounters/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_gp_encounters,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(gp_encounters/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (gp_encounters/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_gp_encounters,
--COST
    -- weighted mean of the annualised rate
    sum(match_weight * (gp_cost / (n_days/365.0))) / sum(match_weight)
    as avg_gp_cost,
    -- weighted SD (pgpulation form via the sum-of-squares identity)
    sqrt(
    sum(match_weight * pow(gp_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (gp_cost/(n_days/365.0))) / sum(match_weight), 2)
    ) as stddev_gp_cost,
    -- standard error using effective N, not the raw row count
    sqrt(
    ( sum(match_weight * pow(gp_cost/(n_days/365.0), 2)) / sum(match_weight)
    - pow(sum(match_weight * (gp_cost/(n_days/365.0))) / sum(match_weight), 2) )
    / ( pow(sum(match_weight), 2) / sum(pow(match_weight, 2)) )
    ) as se_gp_cost
   
from member_annual_costs as m 
 where n_days <> 0
group by    all
order by 
    1, 2


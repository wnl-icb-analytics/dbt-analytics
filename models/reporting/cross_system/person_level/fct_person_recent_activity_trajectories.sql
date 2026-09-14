{{
    config(
        materialized='table')
}}


/*
Patient trajectory data processing for sparklines for current residents or registrants according to PDS

Clinical Purpose:
- Showing activity changes over time

Testing:
- Actual table will use full array of datasets and include measurement observations

*/

with date_spine as(
    select add_months(date_trunc('month',  dateadd(month, -18, current_date())),seq4())::date as activity_month
    FROM TABLE(GENERATOR(ROWCOUNT => 18))
    order by activity_month
),

base_population as(
    select cast(sk_patient_id as varchar) as sk_patient_id
    from {{ ref('dim_person_demographics_basic')}}
    where (flag_current_resident = true or flag_current_registered = true) -- current
    and date_of_death is null -- living
    and sk_patient_id is not null -- valid person
    and sk_patient_id != '1'
), 

inclusion_list as(
    select distinct sk_patient_id
    from base_population
), 
 
activity as(
    select cast(sk_patient_id as varchar) as sk_patient_id
    , activity_month
    , ae_encounters
    , ip_encounters
    , op_encounters
    , gp_encounters
    from {{ref('fct_person_activity_by_month')}}
    where activity_month between
        (select min(activity_month) from date_spine)
        and (select max(activity_month) from date_spine)
)

select il.sk_patient_id
    ,ARRAY_AGG(COALESCE(a.ae_encounters,0)) within group (order by ds.activity_month) as ae_encounters_sl
    ,ARRAY_AGG(COALESCE(a.ip_encounters,0)) within group (order by ds.activity_month) as ip_encounters_sl
    ,ARRAY_AGG(COALESCE(a.op_encounters,0)) within group (order by ds.activity_month) as op_encounters_sl
    ,ARRAY_AGG(COALESCE(a.gp_encounters,0)) within group (order by ds.activity_month) as gp_encounters_sl
from inclusion_list il
cross join date_spine ds
left join activity a 
    on il.sk_patient_id = a.sk_patient_id and ds.activity_month = a.activity_month
group by il.sk_patient_id


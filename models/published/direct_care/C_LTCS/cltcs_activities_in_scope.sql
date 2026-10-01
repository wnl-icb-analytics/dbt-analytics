{{
    config(
        materialized='view',
        enabled=false)
}}


/*
Recent outpatient activities from SUS

Processing:
- build marts for recent (1year) total activity (unfiltered)

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/
with exclusion_activities as (
    select visit_occurrence_id
    from {{ref('int_activity_maternity')}}
    where start_date between dateadd(month, -12, current_date()) and current_date()
    union all
    select visit_occurrence_id
    from {{ref('int_activity_dialysis')}}
    where start_date between dateadd(month, -12, current_date()) and current_date()
    union all
    select visit_occurrence_id
    from {{ref('int_activity_cancer')}}
    where start_date between dateadd(month, -12, current_date()) and current_date()
),
people_in_scope as (
    SELECT PSEUDO_NHS_NUMBER as sk_patient_id, PRIMARY_CARE_PROVIDER
    FROM {{ ref('stg_pds_patient_care_practice') }}
    WHERE REASON_FOR_REMOVAL IS NULL
    AND PRIMARY_CARE_PROVIDER_BUSINESS_EFFECTIVE_TO_DATE IS NULL
    AND PRIMARY_CARE_PROVIDER in ('F83023','F83677','F83632','F83018','F83022','F83057','F85682','F85043','F85023','F85634','F85654','F85072','F85025','Y03402','F85039','E83049','E83053','E83016','E83030')
),
op_cohort as (
    select * 
    from 
        {{ ref('int_sus_op_appointment') }} 
    where 
        start_date between dateadd(month, -12, current_date()) and current_date()
        and sk_patient_id is not null
        and sk_patient_id != 1
        and sk_patient_id in (select distinct sk_patient_id from people_in_scope)
),
op_cohort_in_scope as (
    select * 
    from op_cohort
    where visit_occurrence_id not in (select visit_occurrence_id from exclusion_activities)
),
op_summ as (
    select
        sk_patient_id
        , count(distinct case when appointment_attended_or_dna in ('5', '6') -- Attended
                then visit_occurrence_id end) as op_att_tot_12mo
        , count(distinct visit_occurrence_id) as op_app_tot_12mo
        , count(distinct treatment_function_code) as op_spec_12mo
        , count(distinct organisation_id) as op_prov_12mo
        , ARRAY_UNIQUE_AGG(treatment_function_code_desc) AS tfc_array
    from op_cohort_in_scope
    group by 
        sk_patient_id
),
op_flags as (
    select sk_patient_id,
    CASE WHEN visit_occurrence_id IN (
            SELECT visit_occurrence_id 
            FROM {{ref('int_activity_cancer')}}
        ) THEN 1 ELSE 0 END AS cancer_flag,
     CASE WHEN visit_occurrence_id IN (
            SELECT visit_occurrence_id 
            FROM {{ref('int_activity_dialysis')}}
        ) THEN 1 ELSE 0 END AS dialysis_flag,
     CASE WHEN visit_occurrence_id IN (
            SELECT visit_occurrence_id 
            FROM {{ref('int_activity_maternity')}}
        ) THEN 1 ELSE 0 END AS maternity_flag
    from op_cohort
),
count_of_prov_per_spec as(
    select
        sk_patient_id
        , treatment_function_code
        , count(distinct organisation_id) as op_prov_per_spec_12mo
    from op_cohort_in_scope
    where treatment_function_code is not null 
        and organisation_id is not null
    group by 
        sk_patient_id, treatment_function_code
),
potential_dup_provider as(
    select
        sk_patient_id
        , count(distinct(treatment_function_code)) as op_num_spec_2_prov_12mo
    from 
        count_of_prov_per_spec
    where 
        op_prov_per_spec_12mo > 1
    group by 
        sk_patient_id
)

SELECT a.sk_patient_id
    , pis.PRIMARY_CARE_PROVIDER
    , CASE WHEN a.sk_patient_id IN (
            SELECT DISTINCT sk_patient_id 
            FROM op_flags
            WHERE cancer_flag = 1
        ) THEN 1 ELSE 0 END AS cancer
    , CASE WHEN a.sk_patient_id IN (
            SELECT DISTINCT sk_patient_id 
            FROM op_flags
            WHERE dialysis_flag = 1
        ) THEN 1 ELSE 0 END AS dialysis 
    , CASE WHEN a.sk_patient_id IN (
            SELECT DISTINCT sk_patient_id 
            FROM op_flags
            WHERE maternity_flag = 1
        ) THEN 1 ELSE 0 END AS maternity 
    , tfc_array
    , op_rec.op_att_tot_12mo
    , zeroifnull(a.op_att_tot_12mo) as filt_op_att_tot_12mo
    , zeroifnull(a.op_spec_12mo) as filt_op_spec_12mo
    , zeroifnull(a.op_prov_12mo) as filt_op_prov_12mo 
    , zeroifnull(d.op_num_spec_2_prov_12mo) as filt_op_num_spec_2_prov_12mo
from 
    op_summ as a
left join people_in_scope as pis on a.sk_patient_id = pis.sk_patient_id
left join 
    potential_dup_provider as d 
    on a.sk_patient_id = d.sk_patient_id
left join {{ref('fct_person_sus_op_recent')}} as op_rec on a.sk_patient_id = op_rec.sk_patient_id
where a.sk_patient_id is not null and a.sk_patient_id != 1
and op_rec.op_att_tot_12mo > 1
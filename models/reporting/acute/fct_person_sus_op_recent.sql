{{
    config(
        materialized='view')
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

with op_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when appointment_attended_or_dna in ('5', '6') -- Attended
                then visit_occurrence_id end) as op_att_tot_12mo
        , count(distinct case when appointment_attended_or_dna in ('5', '6') -- Attended
                and start_date between dateadd(month, -3, current_date()) and current_date() 
                then visit_occurrence_id end) as op_att_tot_3mo
        , count(distinct case when appointment_attended_or_dna in ('5', '6') -- Attended
                and start_date between dateadd(month, -1, current_date()) and current_date() 
                then visit_occurrence_id end) as op_att_tot_1mo
        , count(distinct visit_occurrence_id) as op_app_tot_12mo
        , count(distinct case when appointment_attended_or_dna in ('5', '6') -- Attended
                and appointment_first_attendance IN ('1', '3') -- First Appointment
                then visit_occurrence_id end) as op_att_first_12mo
        , count(distinct treatment_function_code) as op_spec_12mo
        , count(distinct organisation_id) as op_prov_12mo
    from 
        {{ ref('int_sus_op_appointment') }}
    where 
        start_date between dateadd(month, -12, current_date()) and current_date()
    group by 
        sk_patient_id
),
count_of_prov_per_spec as(
    select
        sk_patient_id
        , treatment_function_code
        , count(distinct organisation_id) as op_prov_per_spec_12mo
    from 
        {{ ref('int_sus_op_appointment') }}
    where 
        start_date between dateadd(month, -12, current_date()) and current_date()
        and treatment_function_code is not null 
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

SELECT
    a.sk_patient_id
    , zeroifnull(op_att_tot_12mo) as op_att_tot_12mo
    , zeroifnull(op_att_tot_3mo) as op_att_tot_3mo
    , zeroifnull(op_att_tot_1mo) as op_att_tot_1mo
    , zeroifnull(op_att_first_12mo) as op_att_first_12mo
    , zeroifnull(op_app_tot_12mo) as op_app_tot_12mo
    , zeroifnull(op_spec_12mo) as op_spec_12mo
    , zeroifnull(op_prov_12mo) as op_prov_12mo
    , zeroifnull(d.op_num_spec_2_prov_12mo) as op_num_spec_2_prov_12mo
    , zeroifnull(dna.op_dna_opportunities_12mo) as op_dna_opportunities_12mo
    , zeroifnull(dna.op_dna_tot_12mo) as op_dna_tot_12mo
    , dna.op_dna_rate_12mo
    , dna.op_dna_rate_shrunk_12mo
    , dna.op_dna_evidence_weight_12mo
from
    op_encounter_summary as a
left join
    potential_dup_provider as d
    on a.sk_patient_id = d.sk_patient_id
left join
    {{ ref('int_person_sus_op_dna_rate') }} as dna
    on a.sk_patient_id = dna.sk_patient_id
where a.sk_patient_id is not null and a.sk_patient_id != 1


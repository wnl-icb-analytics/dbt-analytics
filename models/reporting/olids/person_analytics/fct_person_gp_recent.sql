
with 
base_encounters as (
    select
        pp.sk_patient_id
        , appointment_id
        , start_date
        , duration_minutes
        , slot_category
        , practitioner_role_group
        , is_attended 
    from {{ ref('int_appointment_gp_clinical') }} gpa
    inner join {{ref('dim_person_pseudo')}} pp on pp.person_id = gpa.person_id
    where start_date between dateadd(month, -12, current_date()) and current_date()
), 
gp_encounter_summary as(
    select
        sk_patient_id
        , count(distinct case when is_attended = TRUE-- Attended (assumed - consider selecting for)
                then appointment_id end) as gp_att_tot_12mo
        , count(distinct case when is_attended = TRUE -- Attended
                and start_date between dateadd(month, -3, current_date()) and current_date() 
                then appointment_id end) as gp_att_tot_3mo
        , count(distinct case when is_attended = TRUE-- Attended
                and start_date between dateadd(month, -1, current_date()) and current_date() 
                then appointment_id end) as gp_att_tot_1mo
        , count(distinct appointment_id) as gp_app_tot_12mo
        , count(distinct case when is_attended = FALSE -- Attended (assumed - consider selecting for)
                then appointment_id end) as gp_dna_tot_12mo
    from base_encounters
    group by 
        sk_patient_id
)

SELECT
    sk_patient_id
    , zeroifnull(gp_att_tot_12mo) as gp_att_tot_12mo
    , zeroifnull(gp_att_tot_3mo) as gp_att_tot_3mo
    , zeroifnull(gp_att_tot_1mo) as gp_att_tot_1mo
    , zeroifnull(gp_app_tot_12mo) as gp_app_tot_12mo
    , zeroifnull(gp_dna_tot_12mo) as gp_dna_tot_12mo
from 
    gp_encounter_summary as a

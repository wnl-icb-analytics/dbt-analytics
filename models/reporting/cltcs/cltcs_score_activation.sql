{{ config(materialized='table', tags=['cltcs']) }}

with inclusion_list as (
    select *
    from {{ ref('cltcs_adult_population') }}
    ),

encoding_features as(
    select il.sk_patient_id
        , il.practice_code
        , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
        , zeroifnull(aea.ae_tot_12mo) + 10 as ae_tot_12mo_plus_10
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) + 10 as gp_att_tot_12mo_plus_10
        , case when pd.main_language in ('English', 'Not Recorded') then 0 else 1 end as main_language_flag -- TO DO: switch to interpreter flag
        , pd.main_language
        , pc.has_severe_mental_illness
        , case when zeroifnull(aea.ae_tot_12mo) > zeroifnull(gpa.gp_att_tot_12mo) then 1 else 0 end as ae_greater_gp_12mo_flag
        , case when pc.has_severe_mental_illness = TRUE then 1 else 0 end as has_severe_mental_illness_flag
        , pc.has_learning_disability
        , case when pc.has_learning_disability = TRUE then 1 else 0 end as has_learning_disability_flag
        , pc.musculoskeletal_conditions
        , case when pc.musculoskeletal_conditions = TRUE then 1 else 0 end as musculoskeletal_conditions_flag
    from inclusion_list il
    left join {{ref('dim_person_demographics')}} pd
        on il.person_id =pd.person_id
    left join {{ref('fct_person_sus_uec_recent')}} aea
        on il.sk_patient_id  =aea.sk_patient_id
    left join {{ref('fct_person_gp_recent')}} gpa
        on il.sk_patient_id  =gpa.sk_patient_id
    left join {{ref('dim_person_conditions')}} pc
        on il.person_id =pc.person_id
)
select sk_patient_id,
    practice_code,
    ae_greater_gp_12mo_flag*5+(ae_tot_12mo_plus_10/gp_att_tot_12mo_plus_10)/2 + main_language_flag*5 + has_severe_mental_illness_flag*10 + has_learning_disability_flag*10 + musculoskeletal_conditions_flag*10 - gp_att_tot_12mo as score_activation
from encoding_features

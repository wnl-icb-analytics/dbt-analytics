{{ config(materialized='table', tags=['mhsds']) }}

with active_contacts as (
    select
        c.uniq_care_cont_id
        , c.uniq_serv_req_id
        , c.mhs201_uniq_id
        , c.care_contact_id
        , c.service_request_id
        , c.person_id
        , c.care_cont_date::date as care_cont_date
        , c.care_cont_time::time as care_cont_time
        , c.org_id_prov
        , c.org_id_comm
        , c.attend_status
        , c.attend_or_dna_code
        , c.clin_cont_dur_of_care_cont
        , c.care_prof_team_local_id
        , c.other_care_prof_team_local_id
        , c.uniq_care_prof_team_id
        , c.uniq_other_care_prof_team_local_id
        , c.site_id_of_treat
        , c.admin_cat_code
        , c.specialised_mh_service_code
        , c.cons_type
        , c.care_cont_subj
        , c.cons_mechanism_mh
        , c.cons_medium_used
        , c.act_loc_type_code
        , c.place_of_safety_ind
        , c.group_therapy_ind
        , c.language_code_treat
        , c.interpreter_present_ind
        , c.com_peri_mh_part_assess_offer_ind
        , c.planned_care_cont_indicator
        , c.care_cont_patient_ther_mode
        -- 1899/1900 values are source missing-date sentinels from Excel epochs.
        , iff(
            c.earliest_reason_offer_date::date < '1901-01-01'::date
            , null
            , c.earliest_reason_offer_date::date
        ) as earliest_reason_offer_date
        , iff(
            c.earliest_clin_app_date::date < '1901-01-01'::date
            , null
            , c.earliest_clin_app_date::date
        ) as earliest_clin_app_date
        , iff(
            c.care_cont_cancel_date::date < '1901-01-01'::date
            , null
            , c.care_cont_cancel_date::date
        ) as care_cont_cancel_date
        , c.care_cont_cancel_reas
        , iff(
            c.rep_appt_offer_date::date < '1901-01-01'::date
            , null
            , c.rep_appt_offer_date::date
        ) as rep_appt_offer_date
        , iff(
            c.rep_appt_book_date::date < '1901-01-01'::date
            , null
            , c.rep_appt_book_date::date
        ) as rep_appt_book_date
        , c.reasonable_adjustment_made
        , c.reason_patient_no_imca
        , c.reason_patient_no_imha
        , c.age_care_cont_date
        , c.cont_loc_distance_home
        , c.time_refer_and_care_contact
        , c.dm_icb_commissioner
        , c.dm_sub_icb_commissioner
        , c.dm_commissioner_derivation_reason
        , c.uniq_submission_id
        , c.uniq_month_id
        , c.reporting_period_start_date::date as reporting_period_start_date
        , c.reporting_period_end_date::date as reporting_period_end_date
        , c.dmic_dataset
        , c.effective_from
        , c.dmic_date_added
    from {{ ref('raw_mhsds_mhs201carecontact') }} as c
    inner join {{ ref('stg_mhsds_activesubmission') }} as a
        on c.uniq_submission_id = a.uniq_submission_id
)

select
    uniq_care_cont_id
    , uniq_serv_req_id
    , mhs201_uniq_id
    , care_contact_id
    , service_request_id
    , person_id
    , care_cont_date
    , care_cont_time
    , org_id_prov
    , org_id_comm
    , attend_status
    , attend_or_dna_code
    , clin_cont_dur_of_care_cont
    , care_prof_team_local_id
    , other_care_prof_team_local_id
    , uniq_care_prof_team_id
    , uniq_other_care_prof_team_local_id
    , site_id_of_treat
    , admin_cat_code
    , specialised_mh_service_code
    , cons_type
    , care_cont_subj
    , cons_mechanism_mh
    , cons_medium_used
    , act_loc_type_code
    , place_of_safety_ind
    , group_therapy_ind
    , language_code_treat
    , interpreter_present_ind
    , com_peri_mh_part_assess_offer_ind
    , planned_care_cont_indicator
    , care_cont_patient_ther_mode
    , earliest_reason_offer_date
    , earliest_clin_app_date
    , care_cont_cancel_date
    , care_cont_cancel_reas
    , rep_appt_offer_date
    , rep_appt_book_date
    , reasonable_adjustment_made
    , reason_patient_no_imca
    , reason_patient_no_imha
    , age_care_cont_date
    , cont_loc_distance_home
    , time_refer_and_care_contact
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , dm_commissioner_derivation_reason
    , uniq_submission_id
    , uniq_month_id
    , reporting_period_start_date
    , reporting_period_end_date
    , dmic_dataset
    , effective_from
    , dmic_date_added
from active_contacts

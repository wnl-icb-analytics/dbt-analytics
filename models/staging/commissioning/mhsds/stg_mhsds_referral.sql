{{ config(materialized='table', tags=['mhsds']) }}

select
    uniq_serv_req_id
    , mhs101_uniq_id
    , service_request_id
    , person_id
    , org_id_prov
    , org_id_comm
    , org_id_referring_org
    , org_id_referring
    , nhs_serv_agree_line_id
    , nhs_serv_agree_line_num
    , specialised_mh_service_code
    , referral_request_received_date
    , referral_request_received_time
    , decision_to_treat_date
    , decision_to_treat_time
    , disch_plan_creation_date
    , disch_plan_creation_time
    , disch_plan_last_updated_date
    , disch_plan_last_updated_time
    , refer_rejection_date
    , refer_rejection_time
    , refer_reject_reason
    , serv_disch_date
    , serv_disch_time
    , refer_clos_reason
    , disch_letter_iss_date
    , source_of_referral_mh
    , referring_care_professional_type
    , referring_care_professional_staff_group
    , clin_resp_priority_type
    , prim_reason_referral_mh
    , reason_oat
    , care_prof_team_local_id
    , uniq_care_prof_team_local_id
    , serv_team_type
    , serv_team_int_age_group
    , service_type_name
    , age_serv_refer_rec_date
    , age_serv_refer_disch_date
    , record_start_date
    , record_end_date
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
from {{ ref('stg_mhsds_referral_history') }} as r
qualify row_number() over (
        partition by r.uniq_serv_req_id
        order by
            r.reporting_period_end_date desc
            , r.effective_from desc
            , r.uniq_submission_id desc
            , r.mhs101_uniq_id desc
    ) = 1

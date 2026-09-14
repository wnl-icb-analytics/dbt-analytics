select
    {{ dbt_utils.generate_surrogate_key([
        'c.uniq_serv_req_id',
        'c.uniq_care_cont_id'
    ]) }} as source_record_id
    , 'MHSDS' as source_dataset
    , c.mhs201_uniq_id
    , c.uniq_care_cont_id
    , c.care_contact_id
    , c.uniq_serv_req_id as referral_source_record_id
    , c.service_request_id
    , c.person_id
    , b.sk_patient_id
    , c.care_cont_date as care_contact_date
    , c.care_cont_time as care_contact_time
    , case
        when c.care_cont_date is null then null
        when c.care_cont_time is null then c.care_cont_date::timestamp_ntz
        else timestamp_ntz_from_parts(c.care_cont_date, c.care_cont_time)
    end as care_contact_at
    , case
        when c.care_cont_date is null then null
        when c.care_cont_time is null then 'date'
        else 'timestamp'
    end as care_contact_time_precision
    , c.clin_cont_dur_of_care_cont as clinical_contact_duration_minutes
    , c.attend_status as attendance_status_code
    , ats.description as attendance_status_description
    , c.cons_type as consultation_type_code
    , consultation_type.description as consultation_type_description
    , c.cons_mechanism_mh as consultation_mechanism_code
    , cm.description as consultation_mechanism_description
    , c.cons_medium_used as consultation_medium_used_code
    , consultation_medium.description as consultation_medium_used_description
    , c.act_loc_type_code as activity_location_type_code
    , alt.description as activity_location_type_description
    , c.care_cont_subj as care_contact_subject_code
    , contact_subject.description as care_contact_subject_description
    , c.planned_care_cont_indicator as planned_care_contact_code
    , planned_contact.description as planned_care_contact_description
    , c.place_of_safety_ind as place_of_safety_code
    , place_of_safety.description as place_of_safety_description
    , c.group_therapy_ind as group_therapy_code
    , group_therapy.description as group_therapy_description
    , c.care_cont_patient_ther_mode as patient_therapy_mode_code
    , therapy_mode.description as patient_therapy_mode_description
    , c.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , c.org_id_comm as commissioner_organisation_code
    , source_commissioner.organisation_name as commissioner_organisation_name
    , c.dm_icb_commissioner as source_derived_icb_commissioner_code
    , coalesce(
        c.dm_icb_commissioner
        , iff(source_commissioner.is_integrated_care_board, c.org_id_comm, null)
    ) as derived_icb_commissioner_code
    , coalesce(
        derived_icb.organisation_name
        , iff(
            source_commissioner.is_integrated_care_board
            , source_commissioner.organisation_name
            , null
        )
    ) as derived_icb_commissioner_name
    , case
        when c.dm_icb_commissioner is not null then 'source_derived'
        when source_commissioner.is_integrated_care_board then 'submitted_icb'
    end as icb_commissioner_derivation_method
    , coalesce(derived_icb.is_wnl_commissioner, false)
        or coalesce(source_commissioner.is_wnl_commissioner, false)
        as is_wnl_commissioner
    , c.dm_sub_icb_commissioner as derived_sub_icb_commissioner_code
    , derived_sub_icb.organisation_name as derived_sub_icb_commissioner_name
    , c.site_id_of_treat as treatment_site_code
    , coalesce(
        treatment_site.organisation_name
        , treatment_service_provider.service_provider_name
    ) as treatment_site_name
    , c.admin_cat_code as administrative_category_code
    , administrative_category.description as administrative_category_description
    , c.specialised_mh_service_code as specialised_mental_health_service_category_code
    , coalesce(
        c.uniq_other_care_prof_team_local_id
        , c.uniq_care_prof_team_id
    ) as service_or_team_id
    , coalesce(
        c.other_care_prof_team_local_id
        , c.care_prof_team_local_id
    ) as service_or_team_local_id
    , td.serv_team_type_mh as service_or_team_type_code
    , tt.description as service_or_team_type_description
    , td.service_type_name as source_service_or_team_type_name
    , td.serv_team_int_age_group as service_or_team_intended_age_group_code
    , intended_age_group.description as service_or_team_intended_age_group_description
    , c.earliest_reason_offer_date as earliest_reasonable_offer_date
    , c.earliest_clin_app_date as earliest_clinically_appropriate_date
    , c.rep_appt_offer_date as appointment_offer_date
    , c.rep_appt_book_date as appointment_booked_date
    , c.care_cont_cancel_date as care_contact_cancellation_date
    , c.care_cont_cancel_reas as care_contact_cancellation_reason_code
    , cancellation_reason.description as care_contact_cancellation_reason_description
    , c.language_code_treat as treatment_language_code
    , treatment_language.description as treatment_language_description
    , c.interpreter_present_ind as interpreter_present_code
    , interpreter_present.description as interpreter_present_description
    , c.com_peri_mh_part_assess_offer_ind
        as community_perinatal_partner_assessment_offer_code
    , perinatal_offer.description
        as community_perinatal_partner_assessment_offer_description
    , c.reasonable_adjustment_made as reasonable_adjustment_made_code
    , reasonable_adjustment.description as reasonable_adjustment_made_description
    , c.reason_patient_no_imca as reason_patient_does_not_have_imca_code
    , no_imca.description as reason_patient_does_not_have_imca_description
    , c.reason_patient_no_imha as reason_patient_does_not_have_imha_code
    , no_imha.description as reason_patient_does_not_have_imha_description
    , c.age_care_cont_date as age_at_care_contact
    , c.cont_loc_distance_home as distance_from_home_km
    , c.time_refer_and_care_contact as minutes_from_referral_to_care_contact
    , c.uniq_submission_id
    , c.uniq_month_id
    , c.reporting_period_start_date
    , c.reporting_period_end_date
    , c.dmic_dataset as mhsds_version
    , c.effective_from as source_file_received_at
    , c.dmic_date_added as source_loaded_at
from {{ ref('int_mhsds_latest_care_contact') }} as c
left join {{ ref('stg_mhsds_bridging') }} as b
    on c.person_id = b.person_id
left join {{ ref('stg_mhsds_service_or_team_details') }} as td
    on coalesce(c.other_care_prof_team_local_id, c.care_prof_team_local_id)
        = td.care_prof_team_local_id
    and c.uniq_submission_id = td.uniq_submission_id
left join {{ ref('attendance_status') }} as ats
    on c.attend_status = ats.code
left join {{ ref('consultation_mechanism') }} as cm
    on c.cons_mechanism_mh = cm.code
left join {{ ref('activity_location_type') }} as alt
    on c.act_loc_type_code = alt.code
left join {{ ref('mhsds_service_or_team_type') }} as tt
    on td.serv_team_type_mh = tt.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as consultation_type
    on upper(trim(c.cons_type)) = consultation_type.code
    and consultation_type.code_set_name = 'consultation_type'
left join {{ ref('mhsds_care_contact_code_lookup') }} as consultation_medium
    on upper(trim(c.cons_medium_used)) = consultation_medium.code
    and consultation_medium.code_set_name = 'consultation_medium_used'
left join {{ ref('mhsds_care_contact_code_lookup') }} as contact_subject
    on upper(trim(c.care_cont_subj)) = contact_subject.code
    and contact_subject.code_set_name = 'care_contact_subject'
left join {{ ref('mhsds_care_contact_code_lookup') }} as planned_contact
    on upper(trim(c.planned_care_cont_indicator)) = planned_contact.code
    and planned_contact.code_set_name = 'planned_care_contact_indicator'
left join {{ ref('mhsds_care_contact_code_lookup') }} as place_of_safety
    on upper(trim(c.place_of_safety_ind)) = place_of_safety.code
    and place_of_safety.code_set_name = 'place_of_safety_indicator'
left join {{ ref('mhsds_care_contact_code_lookup') }} as group_therapy
    on upper(trim(c.group_therapy_ind)) = group_therapy.code
    and group_therapy.code_set_name = 'group_therapy_indicator'
left join {{ ref('mhsds_care_contact_code_lookup') }} as therapy_mode
    on upper(trim(c.care_cont_patient_ther_mode)) = therapy_mode.code
    and therapy_mode.code_set_name = 'patient_therapy_mode'
left join {{ ref('mhsds_care_contact_code_lookup') }} as administrative_category
    on upper(trim(c.admin_cat_code)) = administrative_category.code
    and administrative_category.code_set_name = 'administrative_category'
left join {{ ref('mhsds_service_or_team_intended_age_group') }} as intended_age_group
    on upper(trim(td.serv_team_int_age_group)) = intended_age_group.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as cancellation_reason
    on upper(trim(c.care_cont_cancel_reas)) = cancellation_reason.code
    and cancellation_reason.code_set_name = 'care_contact_cancellation_reason'
left join {{ ref('language') }} as treatment_language
    on upper(trim(c.language_code_treat)) = treatment_language.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as interpreter_present
    on upper(trim(c.interpreter_present_ind)) = interpreter_present.code
    and interpreter_present.code_set_name = 'interpreter_present_indicator'
left join {{ ref('mhsds_care_contact_code_lookup') }} as perinatal_offer
    on upper(trim(c.com_peri_mh_part_assess_offer_ind)) = perinatal_offer.code
    and perinatal_offer.code_set_name = 'perinatal_partner_assessment_offer_indicator'
left join {{ ref('mhsds_care_contact_code_lookup') }} as reasonable_adjustment
    on upper(trim(c.reasonable_adjustment_made)) = reasonable_adjustment.code
    and reasonable_adjustment.code_set_name = 'reasonable_adjustment_made_indicator'
left join {{ ref('mhsds_care_contact_code_lookup') }} as no_imca
    on upper(trim(c.reason_patient_no_imca)) = no_imca.code
    and no_imca.code_set_name = 'reason_patient_no_imca'
left join {{ ref('mhsds_care_contact_code_lookup') }} as no_imha
    on upper(trim(c.reason_patient_no_imha)) = no_imha.code
    and no_imha.code_set_name = 'reason_patient_no_imha'
left join {{ ref('int_mhsds_organisation') }} as source_commissioner
    on upper(c.org_id_comm) = upper(source_commissioner.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(c.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as treatment_site
    on upper(trim(c.site_id_of_treat)) = upper(trim(treatment_site.organisation_code))
left join {{ ref('stg_dictionary_dbo_serviceprovider') }} as treatment_service_provider
    on upper(trim(c.site_id_of_treat))
        = upper(trim(treatment_service_provider.service_provider_code))
left join {{ ref('int_mhsds_organisation') }} as derived_icb
    on upper(c.dm_icb_commissioner) = upper(derived_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as derived_sub_icb
    on upper(c.dm_sub_icb_commissioner) = upper(derived_sub_icb.organisation_code)

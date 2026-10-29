select
    --Patient and Record IDs
    cwt.record_id,
    cwt.sk_patient_id,

    --Geo flag fields
    geo.wnl_geo_flag__trust,
    geo.wnl_geo_flag__registered_at_event,
    geo.wnl_geo_flag__residence_at_event,
    geo.wnl_geo_flag__any,
    geo.ncl_geo_flag__trust,
    geo.ncl_geo_flag__registered_at_event,
    geo.ncl_geo_flag__residence_at_event,
    geo.ncl_geo_flag__any,
    geo.nwl_geo_flag__trust,
    geo.nwl_geo_flag__registered_at_event,
    geo.nwl_geo_flag__residence_at_event,
    geo.nwl_geo_flag__any,

    --Date fields
    cwt.date__consultant_upgrade_date,
    cwt.date__decision_to_refer_date,
    cwt.date__cancer_referral_to_treatment_period_start_date,
    cwt.date__date_first_seen,
    cwt.date__fds_pathway_end_date,
    cwt.date__transfer_to_treatment_date,
    cwt.date__cancer_treatment_period_date,
    cwt.date__treatment_start_date,

    --Organisation fields
    org_fs_trust.organisation_name as org__first_seen_trust_name,
    org_fs_site.organisation_name as org__first_seen_site_name,
    cwt.org__first_seen_trust_code,
    cwt.org__first_seen_site_code,
    geo.wnl_geo_flag__first_seen_trust,
    geo.ncl_geo_flag__first_seen_trust,
    geo.nwl_geo_flag__first_seen_trust,

    org_fds_trust.organisation_name as org__faster_diagnosis_pathway_end_trust_name,
    org_fds_site.organisation_name as org__faster_diagnosis_pathway_end_site_name,
    cwt.org__faster_diagnosis_pathway_end_trust_code,
    cwt.org__faster_diagnosis_pathway_end_site_code,
    geo.wnl_geo_flag__faster_diagnosis_pathway_end_trust,
    geo.ncl_geo_flag__faster_diagnosis_pathway_end_trust,
    geo.nwl_geo_flag__faster_diagnosis_pathway_end_trust,

    org_ppi_trust.organisation_name as org__patient_pathway_identifier_issuer_trust_name,
    cwt.org__patient_pathway_identifier_issuer_trust_code,
    geo.wnl_geo_flag__patient_pathway_identifier_issuer_trust,
    geo.ncl_geo_flag__patient_pathway_identifier_issuer_trust,
    geo.nwl_geo_flag__patient_pathway_identifier_issuer_trust,

    org_con_up_trust.organisation_name as org__consultant_upgrade_trust_name,
    org_con_up_site.organisation_name as org__consultant_upgrade_site_name,
    cwt.org__consultant_upgrade_trust_code,
    cwt.org__consultant_upgrade_site_code,
    geo.wnl_geo_flag__consultant_upgrade_trust,
    geo.ncl_geo_flag__consultant_upgrade_trust,
    geo.nwl_geo_flag__consultant_upgrade_trust,

    org_a_invest_trust.organisation_name as org__accountable_investigating_trust_name,
    cwt.org__accountable_investigating_trust_code,
    geo.wnl_geo_flag__accountable_investigating_trust,
    geo.ncl_geo_flag__accountable_investigating_trust,
    geo.nwl_geo_flag__accountable_investigating_trust,

    org_a_treat_trust.organisation_name as org__accountable_treating_trust_name,
    org_a_treat_site.organisation_name as org__accountable_treating_site_name,
    cwt.org__accountable_treating_trust_code,
    cwt.org__accountable_treating_site_code,
    geo.wnl_geo_flag__accountable_treating_trust,
    geo.ncl_geo_flag__accountable_treating_trust,
    geo.nwl_geo_flag__accountable_treating_trust,

    --Pathway Entry Point
    pep.pathway_entry_point,

    --Referral
    cwt.referral__cancer_referral_type_code,
    ref_crtc.main_description as referral__cancer_referral_type_desc,
    ref_crtc.main_description_shortened as referral__cancer_referral_short,

    cwt.referral__source_of_referral_for_outpatient_code,
    ref_sorfoc.description as referral__source_of_referral_for_outpatient_desc,

    cwt.referral__priority_type_code,
    ref_ptc.description as referral__priority_type_desc,

    --Diagnosis
    cwt.diagnosis__fds_suspected_cancer_type_code,
    diag_fsctc.main_description as diagnosis__fds_suspected_cancer_type_desc,
    
    cwt.diagnosis__fds_end_reason_code,
    diag_ferc.main_description as diagnosis__fds_end_reason_desc,
    
    cwt.diagnosis__fds_end_reason_exclusion_code,
    diag_ferec.main_description as diagnosis__fds_end_reason_exclusion_desc,
    
    cwt.diagnosis__fds_outcome_method_of_communication_code,
    diag_fomocc.main_description as diagnosis__fds_outcome_method_of_communication_desc,
    
    cwt.diagnosis__fds_outcome_profession_type_code,
    diag_foptc.main_description as diagnosis__fds_outcome_profession_type_desc,

    --Treatment
    cwt.treatment__primary_icd10_code,
    treat_ci1c.cancer_site_group as treatment__primary_icd10_group,
    treat_ci1c.description as treatment__primary_icd10_desc,    
    treat_ci1c.cwt_group_hg as treatment__primary_icd10_cancer_type_hg,
    treat_ci1c.cwt_group_lg as treatment__primary_icd10_cancer_type_lg,
    treat_ci1c.stageable_cosd as treatment__primary_icd10_stageable_cosd,
    treat_ci1c.stageable_rcrd as treatment__primary_icd10_stageable_rcrd,
    
    cwt.treatment__modality_code,
    treat_mc.main_description as treatment__modality_desc,
    treat_mc.treatment_group as treatment__modality_group,

    cwt.treatment__event_type_code in ('01', '07', '12') as treatment__flag_first_treatment,
    
    cwt.treatment__event_type_code,
    treat_etc.main_description as treatment__event_type_desc,
    
    cwt.treatment__care_setting_code,
    treat_csc.main_description as treatment__care_setting_desc,

    --Performance fields
    cwt.performance__wta_first_seen_date_adjustment,
    cwt.performance__wta_first_seen_date_reason_code,
    wta_fsd.main_description as performance__wta_first_seen_date_reason_desc, 
    cwt.performance__wta_treatment_start_date_adjustment,
    cwt.performance__wta_treatment_start_date_reason_code,
    wta_tsd.main_description as performance__wta_treatment_start_date_reason_desc,
    
    --Demographics (Static)
    cwt_demo.demographics__gender,
    cwt_demo.demographics__date_of_birth,
    cwt_demo.demographics__date_of_death,
    coalesce(
        cwt_demo.demographics__ethnicity_name,
        'Not Known'
    ) as demographics__ethnicity_name,
    cwt_demo.demographics__ethnicity_subcategory,

    --Demographics (Registration)
    cwt_demo.registered__practice_code,
    cwt_demo.registered__practice_name,
    cwt_demo.registered__practice_end_date,
    cwt_demo.registered__pcn_code,
    cwt_demo.registered__pcn_name,
    cwt_demo.registered__subicb_code,
    cwt_demo.registered__icb_code,
    cwt_demo.registered__icb_name,
    cwt_demo.registered__borough,
    cwt_demo.registered__imd_decile,
    cwt_demo.registered__cancer_alliance_code,
    cwt_demo.registered__cancer_alliance_name,

    --Demographics (Residence)
    cwt_demo.residence__lsoa_code,
    cwt_demo.residence__subicb_code,
    cwt_demo.residence__icb_code,
    cwt_demo.residence__icb_name,
    cwt_demo.residence__borough,
    cwt_demo.residence__imd_decile,
    cwt_demo.residence__neighbourhood_name,
    cwt_demo.residence__cancer_alliance_code,
    cwt_demo.residence__cancer_alliance_name

from {{ ref('int_cwt_pathway__data_tidy') }} cwt

--Join for geo flag columns----------------------------------------------
left join {{ref('int_cwt_pathway__geo_flags')}} geo
on cwt.record_id = geo.record_id
-------------------------------------------------------------------------

--Join to map org codes to names-----------------------------------------
left join {{ ref('stg_dictionary_dbo_organisation') }} org_fs_trust
on cwt.org__first_seen_trust_code = org_fs_trust.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_fs_site
on cwt.org__first_seen_site_code = org_fs_site.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_fds_trust
on cwt.org__faster_diagnosis_pathway_end_trust_code = org_fds_trust.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_fds_site
on cwt.org__faster_diagnosis_pathway_end_site_code = org_fds_site.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_ppi_trust
on cwt.org__patient_pathway_identifier_issuer_trust_code = org_ppi_trust.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_con_up_trust
on cwt.org__consultant_upgrade_trust_code = org_con_up_trust.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_con_up_site
on cwt.org__consultant_upgrade_site_code = org_con_up_site.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_a_invest_trust
on cwt.org__accountable_investigating_trust_code = org_a_invest_trust.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_a_treat_trust
on cwt.org__accountable_treating_trust_code = org_a_treat_trust.organisation_code

left join {{ ref('stg_dictionary_dbo_organisation') }} org_a_treat_site
on cwt.org__accountable_treating_site_code = org_a_treat_site.organisation_code
-------------------------------------------------------------------------

left join {{ ref('int_cwt_pathway__pathway_entry_point')}} pep
on cwt.record_id = pep.record_id

--Referral lookup table joins--------------------------------------------
left join {{ ref('stg_ukhfd_data_dictionary_cancer_usc_referral_type') }} ref_crtc
on cwt.referral__cancer_referral_type_code = ref_crtc.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_source_of_referral_for_out_patients') }} ref_sorfoc
on cwt.referral__source_of_referral_for_outpatient_code = ref_sorfoc.code
and ref_sorfoc.is_latest_definition = true

left join {{ ref('stg_ukhfd_data_dictionary_priority_type') }} ref_ptc
on cwt.referral__priority_type_code = ref_ptc.code
and ref_ptc.is_latest_definition = true
-------------------------------------------------------------------------

--Diagnosis lookup table joins-------------------------------------------
left join {{ ref('stg_ukhfd_data_dictionary_cancer_fdp_suspected_cancer_site') }} diag_fsctc
on cwt.diagnosis__fds_suspected_cancer_type_code = diag_fsctc.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_cancer_fdp_end_reason') }} diag_ferc
on cwt.diagnosis__fds_end_reason_code = diag_ferc.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_cancer_fdp_exclusion_reason') }} diag_ferec
on cwt.diagnosis__fds_end_reason_exclusion_code = diag_ferec.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_cancer_fdp_method_of_communication') }} diag_fomocc
on cwt.diagnosis__fds_outcome_method_of_communication_code = diag_fomocc.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_care_professional_type') }} diag_foptc
on cwt.diagnosis__fds_outcome_method_of_communication_code = diag_foptc.main_code_text
-------------------------------------------------------------------------

--Treatment lookup table joins-------------------------------------------
left join {{ ref('cwt_cancer_icd10_code_groups') }} treat_ci1c
on cwt.treatment__primary_icd10_code = treat_ci1c.code

left join {{ ref('stg_ukhfd_data_dictionary_cancer_treatment_event_type') }} treat_etc
on cwt.treatment__event_type_code = treat_etc.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_cancer_treatment_modality') }} treat_mc
on cwt.treatment__modality_code = treat_mc.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_cancer_treatment_care_setting') }} treat_csc
on cwt.treatment__care_setting_code = treat_csc.main_code_text
-------------------------------------------------------------------------

--Performance adjustment lookup table joins------------------------------
left join {{ ref('stg_ukhfd_data_dictionary_cancer_cwt_waiting_time_adjustment_reason') }} wta_fsd
on cwt.performance__wta_first_seen_date_reason_code = wta_fsd.main_code_text

left join {{ ref('stg_ukhfd_data_dictionary_cancer_cwt_waiting_time_adjustment_reason') }} wta_tsd
on cwt.performance__wta_treatment_start_date_reason_code = wta_tsd.main_code_text
-------------------------------------------------------------------------

--Join for activity demographics-----------------------------------------
left join {{ ref('int_cwt_pathway__demographics')}} cwt_demo
on cwt.record_id = cwt_demo.record_id
-------------------------------------------------------------------------

--Join to get last completed month of data-------------------------------
left join {{ ref('int_cwt_pathway__metadata_period') }} meta
on meta.is_latest = true
-------------------------------------------------------------------------

--Limit to records containing data within the last completed month
where (
    meta.data_period_month >= date_trunc('month', cwt.date__cancer_referral_to_treatment_period_start_date) or
    meta.data_period_month >= date_trunc('month', cwt.date__cancer_treatment_period_date) or
    meta.data_period_month >= date_trunc('month', cwt.date__consultant_upgrade_date) or
    meta.data_period_month >= date_trunc('month', cwt.date__date_first_seen) or
    meta.data_period_month >= date_trunc('month', cwt.date__decision_to_refer_date) or
    meta.data_period_month >= date_trunc('month', cwt.date__fds_pathway_end_date) or
    meta.data_period_month >= date_trunc('month', cwt.date__transfer_to_treatment_date) or
    meta.data_period_month >= date_trunc('month', cwt.date__treatment_start_date)    
)

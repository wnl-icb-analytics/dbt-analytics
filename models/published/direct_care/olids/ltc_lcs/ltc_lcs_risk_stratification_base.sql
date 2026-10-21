{{
    config(
        materialized='table',
        cluster_by=['overall_risk_rank']
    )
}}

-- LTC LCS Risk Stratification Base Table
-- Combines the person-level risk summary with demographics and population health filter flags.
-- One row per adult MoC base member, Check & Test-only LR member or CYP-only asthma child (EMIS CYPAST pathway).

select
    -- ============================================================
    -- Identifiers
    -- ============================================================
    d.person_id,
    d.sk_patient_id,
    pseudo.hx_flake,
    d.postcode_hash,
    d.uprn_hash,

    -- ============================================================
    -- Person status
    -- ============================================================
    d.is_active,
    d.is_deceased,

    -- ============================================================
    -- Demographics: core
    -- ============================================================
    d.gender,
    d.age,
    d.age_band_5y,
    d.age_band_10y,
    d.age_band_nhs,
    d.age_band_esp,
    d.age_life_stage,

    -- ============================================================
    -- Demographics: ethnicity
    -- ============================================================
    d.ethnicity_category,
    d.ethnicity_subcategory,
    d.ethnicity_granular,

    -- ============================================================
    -- Demographics: language
    -- ============================================================
    d.main_language,
    d.language_type,
    d.interpreter_needed,
    d.interpreter_type,

    -- ============================================================
    -- Geography: residence
    -- ============================================================
    d.lsoa_code_21 as lsoa_code,
    d.lsoa_name_21 as lsoa_name,
    d.ward_code,
    d.ward_name,
    d.borough_resident,
    d.neighbourhood_resident,
    d.icb_code_resident,
    d.icb_resident,

    -- ============================================================
    -- Geography: GP practice registration
    -- ============================================================
    d.practice_code,
    d.practice_name,
    d.pcn_code,
    d.pcn_name,
    d.borough_registered,
    d.neighbourhood_registered,

    -- ============================================================
    -- Deprivation
    -- ============================================================
    d.imd_decile_25,
    d.imd_quintile_25,

    -- ============================================================
    -- Age-standardisation weights (ESP 2013)
    -- ============================================================
    d.esp_weight,
    d.esp_proportion,

    -- ============================================================
    -- Population health inclusion filters
    -- ============================================================
    cond.has_learning_disability,
    cond.has_severe_mental_illness,
    coalesce(preg.is_currently_pregnant, false) as is_currently_pregnant,
    ca.person_id is not null as is_complex_adult,
    coalesce(hb.is_housebound, false) as is_housebound,

    -- ============================================================
    -- Risk stratification: per-condition
    -- ============================================================
    rs.af_risk_group,
    rs.asthma_adult_risk_group,
    rs.asthma_cyp_risk_group,
    rs.chd_risk_group,
    rs.ckd_risk_group,
    rs.copd_risk_group,
    rs.diabetes_risk_group,
    rs.hf_risk_group,
    rs.hypertension_risk_group,
    rs.nafld_risk_group,
    rs.pad_risk_group,
    rs.stroke_tia_risk_group,

    -- ============================================================
    -- Risk stratification: overall
    -- ============================================================
    rs.overall_risk_group,
    rs.overall_risk_rank,
    rs.in_any_risk_group,
    rs.number_of_ltc_lcs_conditions,

    -- ============================================================
    -- MOC: pathway identity
    -- ============================================================
    rs.is_in_moc_base,
    rs.moc_base_exclusion_reason,
    rs.is_cyp_asthma_only,
    rs.is_moc_check_test_only,
    rs.is_type_1_diabetes_hrc,
    rs.moc_pathway,
    rs.moc_risk_category,

    -- ============================================================
    -- MOC: activity flags + dates (last 12 months), in pathway order
    -- ============================================================
    rs.moc_mdt_review_completed,
    rs.moc_mdt_review_date,
    rs.moc_stage_1_completed,
    rs.moc_stage_3_completed,
    rs.moc_stage_4_completed,
    rs.moc_requires_check_test,
    rs.moc_requires_stage_2,
    rs.moc_requires_mdt_review,
    rs.moc_requires_careplan_sharing,
    rs.moc_requires_discussion,
    rs.moc_requires_followup,
    rs.moc_ct_overdue,
    rs.moc_discussion_overdue,
    rs.cyp_annual_review_completed,
    rs.cyp_requires_annual_review,
    rs.cyp_requires_desktop_review,
    rs.cyp_desktop_review_completed,
    rs.cyp_annual_review_date,
    rs.cyp_desktop_review_date,
    rs.moc_check_test_completed,
    rs.moc_check_test_date,
    rs.moc_remote_desktop_review_completed,
    rs.moc_remote_desktop_review_date,
    rs.moc_careplan_sharing_completed,
    rs.moc_careplan_sharing_date,
    rs.moc_stage_2_completed,
    rs.moc_stage_2_date,
    rs.moc_discussion_completed,
    rs.moc_discussion_date,
    rs.moc_followup_completed,
    rs.moc_followup_date,
    rs.moc_declined,
    rs.moc_declined_date,
    rs.moc_re_engaged_after_decline,
    rs.moc_any_activity_12m,
    rs.moc_any_activity_fy,

    -- ============================================================
    -- MOC: named progression stages (12m and current FY to date)
    -- ============================================================
    rs.moc_check_test_completed_12m,
    rs.moc_check_test_date_12m,
    rs.moc_check_test_completed_fy,
    rs.moc_check_test_date_fy,
    rs.moc_remote_desktop_review_completed_12m,
    rs.moc_remote_desktop_review_date_12m,
    rs.moc_remote_desktop_review_completed_fy,
    rs.moc_remote_desktop_review_date_fy,
    rs.moc_mdt_review_completed_12m,
    rs.moc_mdt_review_date_12m,
    rs.moc_mdt_review_completed_fy,
    rs.moc_mdt_review_date_fy,
    rs.moc_careplan_sharing_completed_12m,
    rs.moc_careplan_sharing_date_12m,
    rs.moc_careplan_sharing_completed_fy,
    rs.moc_careplan_sharing_date_fy,
    rs.moc_discussion_completed_12m,
    rs.moc_discussion_date_12m,
    rs.moc_discussion_completed_fy,
    rs.moc_discussion_date_fy,
    rs.moc_followup_completed_12m,
    rs.moc_followup_date_12m,
    rs.moc_followup_completed_fy,
    rs.moc_followup_date_fy,

    -- ============================================================
    -- MOC: progression summary
    -- ============================================================
    rs.moc_stage_completed,
    rs.moc_stage_completed_label,
    rs.moc_stage_completed_code,
    rs.moc_pathway_status,
    rs.moc_next_action,
    rs.moc_next_action_code,
    rs.moc_cycle_complete,

    -- ============================================================
    -- MOC: data quality - missing prior stages
    -- ============================================================
    rs.moc_missing_check_test,
    rs.moc_missing_stage_2,
    rs.moc_missing_discussion,
    rs.moc_has_missing_priors,

    -- ============================================================
    -- MOC: stage durations (days)
    -- ============================================================
    rs.moc_days_check_test_to_stage_2,
    rs.moc_days_stage_2_to_discussion,
    rs.moc_days_discussion_to_followup,
    rs.moc_days_check_test_to_followup,

    -- ============================================================
    -- MOC: expiry dates
    -- ============================================================
    rs.moc_careplan_expires_date,
    rs.moc_next_expiry_date,

    -- ============================================================
    -- Outcome: hypertension BP control (ICS_HTN_21; results null when not eligible)
    -- Rolling = current position (last known BP, trailing 12 months);
    -- FY = current financial year YTD (last known BP since 1 April).
    -- ============================================================
    coalesce(oc.is_htn_bp_control_eligible, false) as is_htn_bp_control_eligible,
    oc.htn_bp_controlled_rolling,
    oc.htn_bp_control_rolling_has_reading,
    oc.htn_bp_control_rolling_bp_date,
    oc.htn_bp_control_rolling_systolic,
    oc.htn_bp_control_rolling_diastolic,
    oc.htn_bp_controlled_fy,
    oc.htn_bp_control_fy_has_reading,
    oc.htn_bp_control_fy_bp_date,
    oc.htn_bp_control_fy_systolic,
    oc.htn_bp_control_fy_diastolic,

    -- ============================================================
    -- Metadata
    -- ============================================================
    rs.table_refresh_date
from {{ ref('fct_person_ltc_lcs_risk_summary') }} rs
inner join {{ ref('dim_person_demographics') }} d
    on rs.person_id = d.person_id
left join {{ ref('dim_person_pseudo') }} pseudo
    on rs.person_id = pseudo.person_id
left join {{ ref('dim_person_conditions') }} cond
    on rs.person_id = cond.person_id
left join {{ ref('fct_person_pregnancy_status') }} preg
    on rs.person_id = preg.person_id
left join {{ ref('fct_person_complex_adults') }} ca
    on rs.person_id = ca.person_id
left join {{ ref('dim_person_housebound_status') }} hb
    on rs.person_id = hb.person_id
left join {{ ref('fct_person_ltc_lcs_outcomes') }} oc
    on rs.person_id = oc.person_id

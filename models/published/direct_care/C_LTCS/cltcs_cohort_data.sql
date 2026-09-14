{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Supporting longlisting of patients with complex needs

Testing:
- Actual table have additional fields and pull from a wide array of model outputs
- Replace null handling and time spines with dbt functions in yml

*/

with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
    )

, hr_hrc_ltc_lcs_conditions as (
    select
        person_id
        , chd_risk_group
        , ckd_risk_group
        , copd_risk_group
        , diabetes_risk_group
        , hf_risk_group
        , hypertension_risk_group
        , coalesce(
            array_compact(array_construct(
                case when chd_risk_group in ('HR', 'HRC') then 'CHD' end
                , case when ckd_risk_group in ('HR', 'HRC') then 'CKD' end
                , case when copd_risk_group in ('HR', 'HRC') then 'COPD' end
                , case when diabetes_risk_group in ('HR', 'HRC') then 'Diabetes' end
                , case when hf_risk_group in ('HR', 'HRC') then 'Heart Failure' end
                , case when hypertension_risk_group in ('HR', 'HRC') then 'Hypertension' end
            ))
            , array_construct()
        ) as hr_hrc_ltc_lcs_conditions
        , zeroifnull({{ encode_ltc_lcs_risk_group('chd_risk_group') }}) + zeroifnull({{ encode_ltc_lcs_risk_group('ckd_risk_group') }}) + zeroifnull({{ encode_ltc_lcs_risk_group('copd_risk_group') }}) + zeroifnull({{ encode_ltc_lcs_risk_group('diabetes_risk_group') }}) + zeroifnull({{ encode_ltc_lcs_risk_group('hf_risk_group') }}) + zeroifnull({{ encode_ltc_lcs_risk_group('hypertension_risk_group') }}) as overall_risk_group_sort_key
        , overall_risk_group
        , overall_risk_rank
        , in_any_risk_group
        , moc_stage_completed_label
        , moc_pathway_status
    from {{ ref('fct_person_ltc_lcs_risk_summary') }}
    )

select il.patient_id
      ,{{ hxflake_pseudo_generation('il.patient_id') }} AS re_id_key
    -- , il.fragmented_sk_patient_id_flag -- include as DQ check 
    -- , il.fragmented_person_id_flag  -- include as DQ check 
    , il.area_code
    , coalesce(pd.practice_code, 'Unknown') as practice_code
    , coalesce(pd.practice_name, 'Unknown') as practice_name
    , pd.age 
    , coalesce(pd.main_language, 'Unknown') as main_language
    , coalesce(pd.gender, 'Unknown') as gender
    , coalesce(pd.ethnicity_category, 'Unknown') as ethnicity_category
    , case when pd.main_language is null or pd.main_language in ('English', 'Not Recorded') then 0 else 1 end as main_language_flag -- TO DO: switch to interpreter flag; unknown language treated as no interpreter need (flag 0)
    -- trajectories for sparkline visualisation [add other domains - GP, Community, MH, total?]
    , coalesce(tr.ae_encounters_sl, array_construct()) as ae_encounters_sl
    , coalesce(tr.ip_encounters_sl, array_construct()) as ip_encounters_sl
    , coalesce(tr.op_encounters_sl, array_construct()) as op_encounters_sl
    , coalesce(tr.gp_encounters_sl, array_construct()) as gp_encounters_sl
    -- local disease registries and counts [ only of primary care for now, add acute/community etc data later ]
    , coalesce(pc.has_atrial_fibrillation, false) as has_atrial_fibrillation
    , coalesce(pc.has_asthma, false) as has_asthma
    , coalesce(pc.has_cancer, false) as has_cancer
    , coalesce(pc.has_coronary_heart_disease, false) as has_coronary_heart_disease
    , coalesce(pc.has_chronic_kidney_disease, false) as has_chronic_kidney_disease
    , coalesce(pc.has_copd, false) as has_copd
    , coalesce(pc.has_cyp_asthma, false) as has_cyp_asthma
    , coalesce(pc.has_dementia, false) as has_dementia
    , coalesce(pc.has_depression, false) as has_depression
    , coalesce(pc.has_diabetes, false) as has_diabetes
    , coalesce(pc.has_epilepsy, false) as has_epilepsy
    , coalesce(pc.has_familial_hypercholesterolaemia, false) as has_familial_hypercholesterolaemia
    , coalesce(pc.has_gestational_diabetes, false) as has_gestational_diabetes
    , coalesce(pc.has_frailty, false) as has_frailty
    , coalesce(pc.has_heart_failure, false) as has_heart_failure
    , coalesce(pc.has_hypertension, false) as has_hypertension
    , coalesce(pc.has_learning_disability, false) as has_learning_disability
    , coalesce(pc.has_learning_disability_under_14, false) as has_learning_disability_under_14
    , coalesce(pc.has_nafld, false) as has_nafld
    , coalesce(pc.has_non_diabetic_hyperglycaemia, false) as has_non_diabetic_hyperglycaemia
    , coalesce(pc.has_obesity, false) as has_obesity
    , coalesce(pc.has_osteoporosis, false) as has_osteoporosis
    , coalesce(pc.has_peripheral_arterial_disease, false) as has_peripheral_arterial_disease
    , coalesce(pc.has_palliative_care, false) as has_palliative_care
    , coalesce(pc.has_rheumatoid_arthritis, false) as has_rheumatoid_arthritis
    , coalesce(pc.has_severe_mental_illness, false) as has_severe_mental_illness
    , coalesce(pc.has_stroke_tia, false) as has_stroke_tia
    , coalesce(pc.total_conditions, 0) as total_conditions
    , coalesce(pc.total_qof_conditions, 0) as total_qof_conditions 
    , coalesce(pc.total_non_qof_conditions, 0) as total_non_qof_conditions
    , coalesce(pc.cardiovascular_conditions, 0) as cardiovascular_conditions
    , coalesce(pc.respiratory_conditions, 0) as respiratory_conditions
    , coalesce(pc.mental_health_conditions, 0) as mental_health_conditions
    , coalesce(pc.metabolic_conditions, 0) as metabolic_conditions
    , coalesce(pc.musculoskeletal_conditions, 0) as musculoskeletal_conditions
    , coalesce(pc.neurology_conditions, 0) as neurology_conditions
    , coalesce(pc.geriatric_conditions, 0) as geriatric_conditions
    -- frailty flags: NULL = not assessed, which is clinically distinct from a genuine low/zero score. Do not COALESCE to 0.
    , fr.efi_score
    , coalesce(fr.category, 'Unknown') as efi_category
    , coalesce(rockwood.frailty_level, 'Unknown') as frailty_level
    , coalesce(rockwood.frailty_category, 'Unknown') as frailty_category

    -- mulimorb flags: NULL = not computed.
    , ccms.cambridge_comorbidity_score

    -- risk
    , qrk.qrisk_score
    , qrk.qrisk_type
    , qrk.cvd_risk_category
    , qrk.warrants_statin_consideration -- add actionable flag cross referencing statin prescription
       
      -- Lifestyle and behavioural factors
    , coalesce(br.smoking_status, 'Not Recorded') as smoking_status
    , coalesce(br.smoking_risk_sort_key, 0) as smoking_risk_sort_key
    , coalesce(br.bmi_category, 'Not Recorded') as bmi_category
    , br.bmi_value 
    , coalesce(br.bmi_risk_sort_key, 0) as bmi_risk_sort_key
    , coalesce(br.alcohol_status, 'Not Recorded') as alcohol_status
    , coalesce(br.alcohol_risk_sort_key, 0) as alcohol_risk_sort_key
    -- social care usage
    , coalesce(asc_cld.borough_name, array_construct()) as borough_name
    , coalesce(asc_cld.service_type, array_construct()) as service_type
    , coalesce(asc_cld.primary_support_reason_category, array_construct()) as primary_support_reason_category
    , coalesce(asc_cld.has_physical_support_personal_care, false) as has_physical_support_personal_care
    , coalesce(asc_cld.has_physical_support_access_mobility, false) as has_physical_support_access_mobility
    , coalesce(asc_cld.has_learning_disability_support, false) as has_learning_disability_support
    , coalesce(asc_cld.has_mental_health_support, false) as has_mental_health_support
    , coalesce(asc_cld.has_unknown_primary_support_reason, false) as has_unknown_primary_support_reason
    , coalesce(asc_cld.has_memory_cognition_support, false) as has_memory_cognition_support
    , coalesce(asc_cld.has_social_support_unpaid_carer, false) as has_social_support_unpaid_carer
    , coalesce(asc_cld.has_social_support_social_isolation, false) as has_social_support_social_isolation
    , coalesce(asc_cld.has_sensory_support_visual_impairment, false) as has_sensory_support_visual_impairment
    , coalesce(asc_cld.has_sensory_support_hearing_impairment, false) as has_sensory_support_hearing_impairment
    , coalesce(asc_cld.has_social_support_substance_misuse, false) as has_social_support_substance_misuse
    , coalesce(asc_cld.has_sensory_support_dual_impairment, false) as has_sensory_support_dual_impairment
    , coalesce(asc_cld.has_social_support_asylum_seeker, false) as has_social_support_asylum_seeker
    , coalesce(asc_cld.has_asc_service, 0) as has_asc_service
    , coalesce(ch.is_care_home_resident, false) as is_care_home_resident
    , coalesce(ch.is_nursing_home_resident, false) as is_nursing_home_resident
    , coalesce(ch.is_temporary_resident, false) as is_temporary_resident
    , ch.residence_type 
    , ch.residence_status 
    -- current status to consider
    , coalesce(ps.is_currently_pregnant, false) as is_currently_pregnant
    -- dim_person_is_carer?
    -- bespoke management flags : asthma management flags
    , coalesce(am.testing_no_diagnosis, false) as asthma_testing_no_diagnosis
    , coalesce(am.diagnosis_no_testing, false) as asthma_diagnosis_no_testing
    , coalesce(am.diagnosis_no_act, false) as asthma_diagnosis_no_act
    , coalesce(am.salbutamol_only, false) as asthma_salbutamol_only
    , coalesce(am.salbutamol_repeats, false) as asthma_salbutamol_repeats
    -- measurement flags (fully summaries elsewhere or held as array?)
    , case when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date() then bp.is_overall_bp_controlled else null end as is_overall_bp_controlled -- assuming bp control only relevant if recent, replace with more nuanced logic that ascerts likely control given redings history and time
    ,bp.is_overall_bp_controlled as is_most_recent_overall_bp_controlled
    ,bp.latest_systolic_value 
    ,bp.latest_diastolic_value 
    ,bp.latest_bp_date
    ,coalesce(dcp.care_processes_completed, 0) as care_processes_completed
    , case when dcp.hba1c_completed_in_last_12m = true then dcp.latest_hba1c_value else null end as latest_hba1c_value
    ,dcp.latest_hba1c_date
   -- ,dpr.earliest_type2_date
    -- Annual activity (OP, APC, UEC, MH, ASC, Community, GP appts, 111?)
    ,zeroifnull(opa.op_att_tot_12mo) as op_att_tot_12mo
    ,zeroifnull(opa.op_spec_12mo) as op_spec_12mo
    ,zeroifnull(opa.op_prov_12mo) as op_prov_12mo
    ,zeroifnull(apca.apc_12mo) as apc_12mo
    ,zeroifnull(apca.apc_los_12mo) as apc_los_12mo
    ,zeroifnull(apca.apc_nel_12mo) as apc_nel_12mo
    ,zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo
    ,zeroifnull(aea.ae_t1_12mo) as ae_t1_12mo
    ,zeroifnull(aea.ae_inj_12mo) as ae_inj_12mo
    ,zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
    ,zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
    ,zeroifnull(gpa.gp_app_tot_12mo) as gp_app_tot_12mo
    ,zeroifnull(gpa.gp_dna_tot_12mo) as gp_dna_tot_12mo
    -- existing LTC LCS flags
    , lcs.chd_risk_group
    , lcs.ckd_risk_group
    , lcs.copd_risk_group
    , lcs.diabetes_risk_group
    , lcs.hf_risk_group
    , lcs.hypertension_risk_group
    , coalesce(lcs.hr_hrc_ltc_lcs_conditions, array_construct()) as hr_hrc_ltc_lcs_conditions
    , zeroifnull(lcs.overall_risk_group_sort_key) as overall_risk_group_sort_key
    , coalesce(lcs.overall_risk_group, 'None') as overall_risk_group -- below source default for those not in MOC (LR = lowest risk)
    , coalesce(lcs.overall_risk_rank, 6) as overall_risk_rank -- above source default for those not on MOC as is inverted scale (1 = highest rank)
    , coalesce(lcs.in_any_risk_group, false) as in_any_risk_group
    , lcs.moc_stage_completed_label 
    , lcs.moc_pathway_status 
    -- Current waiting list counts and flags
    ,zeroifnull(wl.wl_current_total_count) as wl_total_count
    ,zeroifnull(wl.wl_current_distinct_providers_count) as wl_provider_count
    ,zeroifnull(wl.wl_current_distinct_tfc_count) as wl_specialty_count
    ,coalesce(wl.same_tfc_multiple_providers_flag, false) as has_same_tfc_multiple_providers_flag
    ,coalesce(wl.current_waiting_list_arrays, array_construct()) as current_waiting_list_arrays
    -- polypharmacy, high risk drugs, suspected non-adherence
    ,zeroifnull(polyp.medication_count) as medication_count
    ,CASE 
        WHEN polyp.medication_name_list IS NULL THEN NULL
        WHEN ARRAY_SIZE(polyp.medication_name_list) = 0 THEN NULL
        ELSE polyp.medication_name_list
      END as medication_name_list
    ,coalesce(polyp.is_polypharmacy_5plus, false) as is_polypharmacy_5plus
    ,coalesce(polyp.is_polypharmacy_10plus, false) as is_polypharmacy_10plus
    , zeroifnull(TO_NUMBER(main_language_flag)) + zeroifnull(TO_NUMBER(has_severe_mental_illness)) + zeroifnull(TO_NUMBER(has_learning_disability)) + zeroifnull(TO_NUMBER(has_asc_service)) as attendance_difficulty_score
    -- Recent medications (last 30 days and last year)
    ,coalesce(rm.medications_recent_12mo, array_construct()) as medications_recent_12mo
    ,zeroifnull(rm.unique_active_ingredient_count_12mo) as unique_active_ingredient_count_12mo
    -- Current referrals

    -- Current risk scores?
    -- scores from cltcs_scores model
    ,coalesce(cs.score_activation, 0) as score_activation
    ,coalesce(cs.score_coordination, 0) as score_coordination
    ,coalesce(cs.score_treatment, 0) as score_treatment
    ,coalesce(cs.score_frailty, 0) as score_frailty
    -- Other relevant annual activity (LTC LCS, C-LTCS review)

from inclusion_list il
left join {{ref('dim_person_demographics')}} pd
    on il.olids_id = pd.person_id
left join {{ ref('fct_person_recent_activity_trajectories') }} tr
    on il.patient_id = tr.sk_patient_id
left join {{ ref('dim_person_conditions')}} pc
    on il.olids_id = pc.person_id
left join {{ref('fct_person_polypharmacy_current')}} polyp
    on il.olids_id = polyp.person_id
left join {{ref('fct_person_behavioural_risk_factors')}} br
    on il.olids_id = br.person_id
left join {{ref('fct_person_pregnancy_status')}} ps
    on il.olids_id = ps.person_id
left join {{ref('fct_person_bp_control')}} bp
    on il.olids_id = bp.person_id 
left join {{ref('fct_person_diabetes_8_care_processes')}} dcp
    on il.olids_id = dcp.person_id
--left join {{ref('fct_person_diabetes_register')}} dpr
--    on il.olids_id  = dpr.person_id
left join {{ref('int_asthma_management')}} am
    on il.olids_id = am.person_id
left join {{ref('fct_person_wl_current_count_total')}} wl
    on il.patient_id = wl.sk_patient_id
left join {{ref('fct_person_sus_op_recent')}} opa
    on il.patient_id  = opa.sk_patient_id
left join {{ref('fct_person_sus_apc_recent')}} apca
    on il.patient_id  = apca.sk_patient_id
left join {{ref('fct_person_sus_uec_recent')}} aea
    on il.patient_id  = aea.sk_patient_id
left join {{ref('fct_person_gp_recent')}} gpa
    on il.patient_id  = gpa.sk_patient_id
left join {{ref('fct_person_medications_recent')}} rm
    on il.olids_id = rm.person_id
left join {{ref('cltcs_scores')}} cs
    on il.patient_id = cs.patient_id
left join {{ref('fct_person_efi2')}} fr
    on il.olids_id = fr.person_id
left join {{ref('dim_person_ccms')}} ccms
    on il.olids_id = ccms.person_id
left join {{ref('int_rockwood_latest')}} rockwood
    on il.olids_id = rockwood.person_id
left join hr_hrc_ltc_lcs_conditions lcs
    on il.olids_id = lcs.person_id
left join {{ref('fct_person_asc_service_recent')}} asc_cld
    on il.patient_id = asc_cld.sk_patient_id
LEFT JOIN {{ref('dim_person_care_home')}} ch
    on il.olids_id = ch.person_id
left join {{ref('int_qrisk_latest')}} qrk
    on il.olids_id = qrk.person_id
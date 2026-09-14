{{
    config(
        materialized='table',
        tags=['cltcs'])
}}

/*
Monthly point-in-time covariate panel for the C-LTCS matched-control candidate pool.

One row per (patient_id, index_date): each control candidate's covariate profile as it
stood on index_date. The spine is the monthly population roster
(cltcs_population_monthly_capture) for the index_date's month -- an INNER join, so only
patients who were in the pool that month get a row (the eligibility gate). Slowly-changing
covariates are read as-at index_date from their SCD2 snapshots via temporal_join (LEFT);
rolling-12-month activity is read from cltcs_activity_monthly_capture for the same month.

Covariate columns and their null-handling mirror cltcs_cohort_data so the two are directly
comparable. Treated-arm covariates are captured separately
(cltcs_cohort_membership_snapshot.attributes_json); this table is the control side. All
patients are kept -- the treated/control split happens at the matching step.

Scope: covariates are limited to those that already have an SCD2 snapshot or the activity
capture. Columns whose history does not yet exist are listed, commented out, in a GAP block
at the end of the SELECT so they are visible as outstanding work. index_date currently uses
placeholder literals for logic testing; swap in the monthly date_spine (commented in the
index_dates CTE) for production.
*/

{%- set index_dates = ['2026-09-01'] -%}
{#-
    v1: placeholder index dates for the logic test (both fall in the 2026-07 roster month).
    Driven by this Jinja list so the spine and the per-date pregnancy reconstruction below
    stay in lock-step. Production: generate a monthly list of month-starts (from 2026-08-01)
    instead -- kept as a compile-time list because the pregnancy loop needs the dates at
    compile time (it cannot consume a runtime dbt_utils.date_spine).
-#}

with index_dates as (
    {%- for d in index_dates %}
    {% if not loop.first %}union all{% endif %}
    select cast('{{ d }}' as date) as index_date
    {%- endfor %}
),

-- Eligibility gate: who was in the control pool in the index_date's month.
spine as (
    select
        i.index_date,
        p.sk_patient_id,
        p.person_id,
        p.neighbourhood_code
    from index_dates i
    join {{ ref('cltcs_population_monthly_capture') }} p
      on p.snapshot_month = date_trunc('month', i.index_date)
    where p.sk_patient_id is not null and p.sk_patient_id <> '1'
),

-- Covariate CTEs: LEFT temporal_join, each version read as-at the row's index_date.
demographics as (
    -- practice_name is NOT in the demographics snapshot input -> gap (see GAP block).
    -- age is deliberately NOT snapshotted (it changes every build); it is computed from live DOB
    -- (the dob CTE) as-at index_date in the final SELECT.
    select f.sk_patient_id, f.index_date,
           d.practice_code, d.main_language, d.gender, d.ethnicity_category
    from {{ temporal_join('spine', 'index_date', ref('dim_person_demographics_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

-- Date of birth is immutable, so it needs no snapshot: read it live from dim_person_demographics
-- and compute age as-at index_date below (person-keyed, one DOB per person).
dob as (
    select person_id, birth_date_approx
    from {{ ref('dim_person_demographics') }}
),

-- practice_name is a label of the practice (not per-person as-at state), so it needs no snapshot:
-- look it up from the code->name reference (dim_practice, one row per practice_code) on the as-at
-- practice_code the demographics CTE already resolves (mirrors the area_code / age approach).
practice as (
    select practice_code, practice_name
    from {{ ref('dim_practice') }}
),

conditions as (
    select f.sk_patient_id, f.index_date,
           d.has_atrial_fibrillation, d.has_asthma, d.has_cancer, d.has_coronary_heart_disease,
           d.has_chronic_kidney_disease, d.has_copd, d.has_cyp_asthma, d.has_dementia,
           d.has_depression, d.has_diabetes, d.has_epilepsy, d.has_familial_hypercholesterolaemia,
           d.has_gestational_diabetes, d.has_frailty, d.has_heart_failure, d.has_hypertension,
           d.has_learning_disability, d.has_learning_disability_under_14, d.has_nafld,
           d.has_non_diabetic_hyperglycaemia, d.has_obesity, d.has_osteoporosis,
           d.has_peripheral_arterial_disease, d.has_palliative_care, d.has_rheumatoid_arthritis,
           d.has_severe_mental_illness, d.has_stroke_tia,
           d.total_conditions, d.total_qof_conditions, d.total_non_qof_conditions,
           d.cardiovascular_conditions, d.respiratory_conditions, d.mental_health_conditions,
           d.metabolic_conditions, d.musculoskeletal_conditions, d.neurology_conditions,
           d.geriatric_conditions
    from {{ temporal_join('spine', 'index_date', ref('dim_person_conditions_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

polypharmacy as (
    select f.sk_patient_id, f.index_date,
           d.medication_count, d.medication_name_list,
           d.is_polypharmacy_5plus, d.is_polypharmacy_10plus
    from {{ temporal_join('spine', 'index_date', ref('fct_person_polypharmacy_current_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

behavioural_risk as (
    select f.sk_patient_id, f.index_date,
           d.smoking_status, d.smoking_risk_sort_key, d.bmi_category, d.bmi_value,
           d.bmi_risk_sort_key, d.alcohol_status, d.alcohol_risk_sort_key
    from {{ temporal_join('spine', 'index_date', ref('fct_person_behavioural_risk_factors_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

ccms as (
    select f.sk_patient_id, f.index_date,
           d.cambridge_comorbidity_score
    from {{ temporal_join('spine', 'index_date', ref('dim_person_ccms_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

care_home as (
    select f.sk_patient_id, f.index_date,
           d.is_care_home_resident, d.is_nursing_home_resident, d.is_temporary_resident,
           d.residence_type, d.residence_status
    from {{ temporal_join('spine', 'index_date', ref('dim_person_care_home_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

-- Frailty (as-at index_date). eFI2 and Rockwood are kept as two separate sources, mirroring
-- cltcs_cohort_data: eFI2 score/category from fct_person_efi2_snapshot (thin input drops the
-- CURRENT_DATE()-derived end_date/age_at_end), Rockwood level/category from
-- int_rockwood_latest_snapshot. Both keyed on person_id.
efi2 as (
    select f.sk_patient_id, f.index_date, d.efi_score, d.category
    from {{ temporal_join('spine', 'index_date', ref('fct_person_efi2_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

rockwood as (
    select f.sk_patient_id, f.index_date, d.frailty_level, d.frailty_category
    from {{ temporal_join('spine', 'index_date', ref('int_rockwood_latest_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

risk_summary as (
    select f.sk_patient_id, f.index_date,
           d.chd_risk_group, d.ckd_risk_group, d.copd_risk_group, d.diabetes_risk_group,
           d.hf_risk_group, d.hypertension_risk_group, d.overall_risk_group,
           d.overall_risk_rank, d.in_any_risk_group,
           d.moc_stage_completed_label, d.moc_pathway_status
    from {{ temporal_join('spine', 'index_date', ref('fct_person_ltc_lcs_risk_summary_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

score_treatment as (
    select f.sk_patient_id, f.index_date, d.score_treatment
    from {{ temporal_join('spine', 'index_date', ref('cltcs_score_treatment_snapshot'),
                          join_key='sk_patient_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

score_frailty as (
    select f.sk_patient_id, f.index_date, d.score_frailty
    from {{ temporal_join('spine', 'index_date', ref('cltcs_score_frailty_snapshot'),
                          join_key='sk_patient_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

score_activation as (
    select f.sk_patient_id, f.index_date, d.score_activation
    from {{ temporal_join('spine', 'index_date', ref('cltcs_score_activation_snapshot'),
                          join_key='sk_patient_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

score_coordination as (
    select f.sk_patient_id, f.index_date, d.score_coordination
    from {{ temporal_join('spine', 'index_date', ref('cltcs_score_coordination_snapshot'),
                          join_key='sk_patient_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

-- Rolling-12-month activity, captured monthly: equijoin on the month (not temporal).
activity as (
    select s.sk_patient_id, s.index_date,
           a.ae_tot_12mo, a.ae_t1_12mo, a.ae_inj_12mo,
           a.apc_12mo, a.apc_nel_12mo, a.apc_los_12mo, a.acs_nel_12mo,
           a.op_att_tot_12mo, a.op_spec_12mo, a.op_prov_12mo,
           a.gp_att_tot_12mo, a.gp_app_tot_12mo, a.gp_dna_tot_12mo,
           a.wl_current_total_count, a.wl_current_distinct_providers_count,
           a.wl_current_distinct_tfc_count,
           a.same_tfc_multiple_providers_flag, a.current_waiting_list_arrays
    from spine s
    left join {{ ref('cltcs_activity_monthly_capture') }} a
      on  a.sk_patient_id = s.sk_patient_id
      and a.snapshot_month = date_trunc('month', s.index_date)
),

-- Pregnancy status: no snapshot, so reconstruct per index date by calling the
-- pregnancy_status_history() macro once per date (start == end) and unioning, stamping
-- the index_date. Keyed on person_id.
pregnancy as (
    {%- for d in index_dates %}
    {% if not loop.first %}union all{% endif %}
    select person_id, cast('{{ d }}' as date) as index_date, was_pregnant
    from (
        {{ pregnancy_status_history(start_date=d, end_date=d) }}
    )
    {%- endfor %}
),

-- BP control: as-at read of the thin BP-control snapshot (flag + latest reading).
bp_control as (
    select f.sk_patient_id, f.index_date,
           d.is_overall_bp_controlled, d.latest_systolic_value,
           d.latest_diastolic_value, d.latest_bp_date
    from {{ temporal_join('spine', 'index_date', ref('fct_person_bp_control_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

-- Diabetes care processes: as-at read of the STABLE per-reading inputs (latest measurement
-- dates + hba1c value + a date-independent foot-check qualifier). The care-processes model is
-- rolling-12-month (anchored on CURRENT_DATE), so only its stable inputs are snapshotted; the
-- 12-month completion window is re-applied on index_date in the final SELECT (like BP's recency
-- gate), reproducing the 8-process count and hba1c recency as-at index_date rather than the
-- snapshot run date. (latest_retinal_screening_date is in the snapshot for future use.)
diabetes as (
    select f.sk_patient_id, f.index_date,
           d.latest_hba1c_date, d.latest_hba1c_value,
           d.latest_bp_date, d.latest_cholesterol_date, d.latest_creatinine_date,
           d.latest_acr_date, d.latest_foot_check_date, d.latest_bmi_date,
           d.latest_smoking_date, d.foot_check_qualifies
    from {{ temporal_join('spine', 'index_date', ref('fct_person_diabetes_9_care_processes_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

-- QRISK CVD risk: as-at read of the latest-valid-QRISK snapshot. Every column is a pure
-- function of the reading (no CURRENT_DATE), so it reads in straight (no re-derivation, no
-- recency gate) and passes through NULL when absent -- matching cltcs_cohort_data.
qrisk as (
    select f.sk_patient_id, f.index_date,
           d.qrisk_score, d.qrisk_type, d.cvd_risk_category, d.warrants_statin_consideration
    from {{ temporal_join('spine', 'index_date', ref('int_qrisk_latest_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
),

-- Asthma management: no snapshot, reconstruct per index date via asthma_management_history()
-- (as_of == index date). Keyed on person_id. Macro returns all 7 flags; we select the 5 used.
asthma as (
    {%- for d in index_dates %}
    {% if not loop.first %}union all{% endif %}
    select
        person_id,
        cast('{{ d }}' as date) as index_date,
        testing_no_diagnosis,
        diagnosis_no_testing,
        diagnosis_no_act,
        salbutamol_only,
        salbutamol_repeats
    from (
        {{ asthma_management_history(as_of="cast('" ~ d ~ "' as date)") }}
    )
    {%- endfor %}
),

-- Adult social care: rolling "recent" ASC services, captured monthly (like activity);
-- equijoin on the month, not temporal. has_asc_service = 1 when a capture row exists.
asc_service as (
    select s.sk_patient_id, s.index_date,
           a.has_asc_service,
           a.borough_name, a.service_type, a.primary_support_reason_category,
           a.has_physical_support_personal_care, a.has_physical_support_access_mobility,
           a.has_learning_disability_support, a.has_mental_health_support,
           a.has_unknown_primary_support_reason, a.has_memory_cognition_support,
           a.has_social_support_unpaid_carer, a.has_social_support_social_isolation,
           a.has_sensory_support_visual_impairment, a.has_sensory_support_hearing_impairment,
           a.has_social_support_substance_misuse, a.has_sensory_support_dual_impairment,
           a.has_social_support_asylum_seeker
    from spine s
    left join {{ ref('cltcs_asc_monthly_capture') }} a
      on  a.sk_patient_id = s.sk_patient_id
      and a.snapshot_month = date_trunc('month', s.index_date)
),

-- Recent medications: rolling "recent" prescriptions, captured monthly (like activity/ASC);
-- equijoin on the month, keyed on person_id (like the pregnancy/asthma joins).
meds as (
    select s.person_id, s.index_date,
           m.medications_recent_12mo, m.unique_active_ingredient_count_12mo
    from spine s
    left join {{ ref('cltcs_medications_monthly_capture') }} m
      on  m.person_id = s.person_id
      and m.snapshot_month = date_trunc('month', s.index_date)
),

-- Resident IMD 2025 quintile (as-at index_date), from the int_person_geography snapshot. Net-new
-- covariate (not in cltcs_cohort_data): matched as a separate stratum (controls here, treated read
-- live at matching time). int_person_geography collapses each person to a single, ~94%-undated
-- address, so this tracks residence forward from the snapshot's first run, not true past residence
-- (the value is "most-recently-known residence", not period-accurate deprivation).
geography as (
    select f.sk_patient_id, f.index_date, d.imd_quintile_25
    from {{ temporal_join('spine', 'index_date', ref('int_person_geography_snapshot'),
                          join_key='person_id', join_type='left',
                          valid_from_col='dbt_valid_from', valid_to_col='dbt_valid_to') }}
)

select
    -- keys / grain
      s.sk_patient_id as patient_id
    , s.person_id as person_id
    , {{ hxflake_pseudo_generation('s.sk_patient_id') }} as re_id_key
    , s.index_date
    , s.neighbourhood_code
    -- cltcs_cohort_data's area_code is the same nh_gp-mapping neighbourhood_code (legacy name); alias it
    , s.neighbourhood_code as area_code
    -- resident IMD 2025 quintile (text label) as-at index_date; NULL when residence unknown. Net-new
    -- covariate (not in cltcs_cohort_data), matched as a separate stratum -- see the geography CTE.
    , geo.imd_quintile_25
    -- demographics (as-at)
    , coalesce(dem.practice_code, 'Unknown') as practice_code
    -- practice_name resolved from the as-at practice_code via dim_practice (current canonical name)
    , coalesce(pr.practice_name, 'Unknown') as practice_name
    -- age at index_date from live (immutable) DOB; same formula as dim_person_demographics.age, anchored on index_date
    , case when dob.birth_date_approx is not null
           then floor(datediff(month, dob.birth_date_approx, s.index_date) / 12)
           else null end as age
    , coalesce(dem.main_language, 'Unknown') as main_language
    , coalesce(dem.gender, 'Unknown') as gender
    , coalesce(dem.ethnicity_category, 'Unknown') as ethnicity_category
    , case when dem.main_language is null or dem.main_language in ('English', 'Not Recorded') then 0 else 1 end as main_language_flag
    -- local disease registries and counts
    , coalesce(con.has_atrial_fibrillation, false) as has_atrial_fibrillation
    , coalesce(con.has_asthma, false) as has_asthma
    , coalesce(con.has_cancer, false) as has_cancer
    , coalesce(con.has_coronary_heart_disease, false) as has_coronary_heart_disease
    , coalesce(con.has_chronic_kidney_disease, false) as has_chronic_kidney_disease
    , coalesce(con.has_copd, false) as has_copd
    , coalesce(con.has_cyp_asthma, false) as has_cyp_asthma
    , coalesce(con.has_dementia, false) as has_dementia
    , coalesce(con.has_depression, false) as has_depression
    , coalesce(con.has_diabetes, false) as has_diabetes
    , coalesce(con.has_epilepsy, false) as has_epilepsy
    , coalesce(con.has_familial_hypercholesterolaemia, false) as has_familial_hypercholesterolaemia
    , coalesce(con.has_gestational_diabetes, false) as has_gestational_diabetes
    , coalesce(con.has_frailty, false) as has_frailty
    , coalesce(con.has_heart_failure, false) as has_heart_failure
    , coalesce(con.has_hypertension, false) as has_hypertension
    , coalesce(con.has_learning_disability, false) as has_learning_disability
    , coalesce(con.has_learning_disability_under_14, false) as has_learning_disability_under_14
    , coalesce(con.has_nafld, false) as has_nafld
    , coalesce(con.has_non_diabetic_hyperglycaemia, false) as has_non_diabetic_hyperglycaemia
    , coalesce(con.has_obesity, false) as has_obesity
    , coalesce(con.has_osteoporosis, false) as has_osteoporosis
    , coalesce(con.has_peripheral_arterial_disease, false) as has_peripheral_arterial_disease
    , coalesce(con.has_palliative_care, false) as has_palliative_care
    , coalesce(con.has_rheumatoid_arthritis, false) as has_rheumatoid_arthritis
    , coalesce(con.has_severe_mental_illness, false) as has_severe_mental_illness
    , coalesce(con.has_stroke_tia, false) as has_stroke_tia
    , coalesce(con.total_conditions, 0) as total_conditions
    , coalesce(con.total_qof_conditions, 0) as total_qof_conditions
    , coalesce(con.total_non_qof_conditions, 0) as total_non_qof_conditions
    , coalesce(con.cardiovascular_conditions, 0) as cardiovascular_conditions
    , coalesce(con.respiratory_conditions, 0) as respiratory_conditions
    , coalesce(con.mental_health_conditions, 0) as mental_health_conditions
    , coalesce(con.metabolic_conditions, 0) as metabolic_conditions
    , coalesce(con.musculoskeletal_conditions, 0) as musculoskeletal_conditions
    , coalesce(con.neurology_conditions, 0) as neurology_conditions
    , coalesce(con.geriatric_conditions, 0) as geriatric_conditions
    -- frailty (as-at index_date): eFI2 + Rockwood, kept separate to mirror cltcs_cohort_data.
    -- efi_score left NULL (not assessed is distinct from a low score); categories -> 'Unknown'.
    , ef.efi_score
    , coalesce(ef.category, 'Unknown') as efi_category
    , coalesce(rk.frailty_level, 'Unknown') as frailty_level
    , coalesce(rk.frailty_category, 'Unknown') as frailty_category
    -- multimorbidity (NULL = not computed)
    , ccm.cambridge_comorbidity_score
    -- lifestyle and behavioural factors
    , coalesce(br.smoking_status, 'Not Recorded') as smoking_status
    , coalesce(br.smoking_risk_sort_key, 0) as smoking_risk_sort_key
    , coalesce(br.bmi_category, 'Not Recorded') as bmi_category
    , br.bmi_value
    , coalesce(br.bmi_risk_sort_key, 0) as bmi_risk_sort_key
    , coalesce(br.alcohol_status, 'Not Recorded') as alcohol_status
    , coalesce(br.alcohol_risk_sort_key, 0) as alcohol_risk_sort_key
    -- social care usage (ASC), as-at the captured month
    , coalesce(asc_c.borough_name, array_construct()) as borough_name
    , coalesce(asc_c.service_type, array_construct()) as service_type
    , coalesce(asc_c.primary_support_reason_category, array_construct()) as primary_support_reason_category
    , coalesce(asc_c.has_physical_support_personal_care, false) as has_physical_support_personal_care
    , coalesce(asc_c.has_physical_support_access_mobility, false) as has_physical_support_access_mobility
    , coalesce(asc_c.has_learning_disability_support, false) as has_learning_disability_support
    , coalesce(asc_c.has_mental_health_support, false) as has_mental_health_support
    , coalesce(asc_c.has_unknown_primary_support_reason, false) as has_unknown_primary_support_reason
    , coalesce(asc_c.has_memory_cognition_support, false) as has_memory_cognition_support
    , coalesce(asc_c.has_social_support_unpaid_carer, false) as has_social_support_unpaid_carer
    , coalesce(asc_c.has_social_support_social_isolation, false) as has_social_support_social_isolation
    , coalesce(asc_c.has_sensory_support_visual_impairment, false) as has_sensory_support_visual_impairment
    , coalesce(asc_c.has_sensory_support_hearing_impairment, false) as has_sensory_support_hearing_impairment
    , coalesce(asc_c.has_social_support_substance_misuse, false) as has_social_support_substance_misuse
    , coalesce(asc_c.has_sensory_support_dual_impairment, false) as has_sensory_support_dual_impairment
    , coalesce(asc_c.has_social_support_asylum_seeker, false) as has_social_support_asylum_seeker
    , coalesce(asc_c.has_asc_service, 0) as has_asc_service
    -- derived attendance-difficulty score (depends on has_asc_service, now available). Inputs
    -- main_language_flag / has_severe_mental_illness / has_learning_disability are aliases from
    -- earlier in this SELECT; has_asc_service from the asc_service CTE above.
    , zeroifnull(to_number(main_language_flag)) + zeroifnull(to_number(has_severe_mental_illness))
      + zeroifnull(to_number(has_learning_disability)) + zeroifnull(to_number(has_asc_service))
      as attendance_difficulty_score
    -- care home / residence
    , coalesce(ch.is_care_home_resident, false) as is_care_home_resident
    , coalesce(ch.is_nursing_home_resident, false) as is_nursing_home_resident
    , coalesce(ch.is_temporary_resident, false) as is_temporary_resident
    , ch.residence_type
    , ch.residence_status
    -- current status
    , coalesce(preg.was_pregnant, false) as is_currently_pregnant
    -- asthma management flags (reconstructed as-at index_date)
    , coalesce(am.testing_no_diagnosis, false) as asthma_testing_no_diagnosis
    , coalesce(am.diagnosis_no_testing, false) as asthma_diagnosis_no_testing
    , coalesce(am.diagnosis_no_act, false) as asthma_diagnosis_no_act
    , coalesce(am.salbutamol_only, false) as asthma_salbutamol_only
    , coalesce(am.salbutamol_repeats, false) as asthma_salbutamol_repeats
    -- BP control (readings left NULL; is_overall_bp_controlled recency-gated to 6 months of index_date)
    , case when bp.latest_bp_date between dateadd(month, -6, s.index_date) and s.index_date
           then bp.is_overall_bp_controlled else null end as is_overall_bp_controlled
    , bp.is_overall_bp_controlled as is_most_recent_overall_bp_controlled
    , bp.latest_systolic_value
    , bp.latest_diastolic_value
    , bp.latest_bp_date
    -- diabetes 8 care processes (rolling 12-month completion window re-anchored on index_date)
    , (
          case when dia.latest_hba1c_date       between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
        + case when dia.latest_bp_date          between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
        + case when dia.latest_cholesterol_date between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
        + case when dia.latest_creatinine_date  between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
        + case when dia.latest_acr_date         between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
        + case when dia.latest_foot_check_date  between dateadd(month, -12, s.index_date) and s.index_date and dia.foot_check_qualifies then 1 else 0 end
        + case when dia.latest_bmi_date         between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
        + case when dia.latest_smoking_date     between dateadd(month, -12, s.index_date) and s.index_date then 1 else 0 end
      ) as care_processes_completed
    , case when dia.latest_hba1c_date between dateadd(month, -12, s.index_date) and s.index_date
           then dia.latest_hba1c_value else null end as latest_hba1c_value
    , dia.latest_hba1c_date
    -- CVD risk (QRISK): latest valid score as-at index_date; passthrough, NULL when absent
    , qr.qrisk_score
    , qr.qrisk_type
    , qr.cvd_risk_category
    , qr.warrants_statin_consideration
    -- annual activity (as-at month, from the activity capture)
    , zeroifnull(act.op_att_tot_12mo) as op_att_tot_12mo
    , zeroifnull(act.op_spec_12mo) as op_spec_12mo
    , zeroifnull(act.op_prov_12mo) as op_prov_12mo
    , zeroifnull(act.apc_12mo) as apc_12mo
    , zeroifnull(act.apc_los_12mo) as apc_los_12mo
    , zeroifnull(act.apc_nel_12mo) as apc_nel_12mo
    , zeroifnull(act.acs_nel_12mo) as acs_nel_12mo
    , zeroifnull(act.ae_t1_12mo) as ae_t1_12mo
    , zeroifnull(act.ae_inj_12mo) as ae_inj_12mo
    , zeroifnull(act.ae_tot_12mo) as ae_tot_12mo
    , zeroifnull(act.gp_att_tot_12mo) as gp_att_tot_12mo
    , zeroifnull(act.gp_app_tot_12mo) as gp_app_tot_12mo
    , zeroifnull(act.gp_dna_tot_12mo) as gp_dna_tot_12mo
    -- LTC LCS risk groups + derived
    , rs.chd_risk_group
    , rs.ckd_risk_group
    , rs.copd_risk_group
    , rs.diabetes_risk_group
    , rs.hf_risk_group
    , rs.hypertension_risk_group
    , coalesce(
        array_compact(array_construct(
              case when rs.chd_risk_group in ('HR', 'HRC') then 'CHD' end
            , case when rs.ckd_risk_group in ('HR', 'HRC') then 'CKD' end
            , case when rs.copd_risk_group in ('HR', 'HRC') then 'COPD' end
            , case when rs.diabetes_risk_group in ('HR', 'HRC') then 'Diabetes' end
            , case when rs.hf_risk_group in ('HR', 'HRC') then 'Heart Failure' end
            , case when rs.hypertension_risk_group in ('HR', 'HRC') then 'Hypertension' end
        ))
        , array_construct()
      ) as hr_hrc_ltc_lcs_conditions
    , zeroifnull({{ encode_ltc_lcs_risk_group('rs.chd_risk_group') }})
      + zeroifnull({{ encode_ltc_lcs_risk_group('rs.ckd_risk_group') }})
      + zeroifnull({{ encode_ltc_lcs_risk_group('rs.copd_risk_group') }})
      + zeroifnull({{ encode_ltc_lcs_risk_group('rs.diabetes_risk_group') }})
      + zeroifnull({{ encode_ltc_lcs_risk_group('rs.hf_risk_group') }})
      + zeroifnull({{ encode_ltc_lcs_risk_group('rs.hypertension_risk_group') }}) as overall_risk_group_sort_key
    , coalesce(rs.overall_risk_group, 'None') as overall_risk_group
    , coalesce(rs.overall_risk_rank, 6) as overall_risk_rank
    , coalesce(rs.in_any_risk_group, false) as in_any_risk_group
    , rs.moc_stage_completed_label
    , rs.moc_pathway_status
    -- waiting list counts + flag + arrays (captured monthly; values as-at the captured month)
    , zeroifnull(act.wl_current_total_count) as wl_total_count
    , zeroifnull(act.wl_current_distinct_providers_count) as wl_provider_count
    , zeroifnull(act.wl_current_distinct_tfc_count) as wl_specialty_count
    , coalesce(act.same_tfc_multiple_providers_flag, false) as has_same_tfc_multiple_providers_flag
    , coalesce(act.current_waiting_list_arrays, array_construct()) as current_waiting_list_arrays
    -- polypharmacy
    , zeroifnull(poly.medication_count) as medication_count
    , case
        when poly.medication_name_list is null then null
        when array_size(poly.medication_name_list) = 0 then null
        else poly.medication_name_list
      end as medication_name_list
    , coalesce(poly.is_polypharmacy_5plus, false) as is_polypharmacy_5plus
    , coalesce(poly.is_polypharmacy_10plus, false) as is_polypharmacy_10plus
    -- recent medications (last year), as-at the captured month
    , coalesce(med.medications_recent_12mo, array_construct()) as medications_recent_12mo
    , zeroifnull(med.unique_active_ingredient_count_12mo) as unique_active_ingredient_count_12mo
    -- C-LTCS scores (all four sub-scores, as-at index_date)
    , coalesce(sa.score_activation, 0) as score_activation
    , coalesce(sco.score_coordination, 0) as score_coordination
    , coalesce(st.score_treatment, 0) as score_treatment
    , coalesce(sf.score_frailty, 0) as score_frailty



    -- ============================================================================
    -- GAP COLUMNS -- present in cltcs_cohort_data but not (yet) emitted here.
    -- Trajectories (sparkline arrays) -- dropped from scope
    -- , ae_encounters_sl, ip_encounters_sl, op_encounters_sl, gp_encounters_sl
    -- ============================================================================

from spine s
left join demographics     dem on dem.sk_patient_id = s.sk_patient_id and dem.index_date = s.index_date
left join dob              on dob.person_id = s.person_id
left join practice         pr  on pr.practice_code = dem.practice_code
left join conditions       con on con.sk_patient_id = s.sk_patient_id and con.index_date = s.index_date
left join polypharmacy     poly on poly.sk_patient_id = s.sk_patient_id and poly.index_date = s.index_date
left join behavioural_risk br  on br.sk_patient_id = s.sk_patient_id and br.index_date = s.index_date
left join ccms             ccm on ccm.sk_patient_id = s.sk_patient_id and ccm.index_date = s.index_date
left join care_home        ch  on ch.sk_patient_id = s.sk_patient_id and ch.index_date = s.index_date
left join efi2             ef  on ef.sk_patient_id = s.sk_patient_id and ef.index_date = s.index_date
left join rockwood         rk  on rk.sk_patient_id = s.sk_patient_id and rk.index_date = s.index_date
left join risk_summary     rs  on rs.sk_patient_id = s.sk_patient_id and rs.index_date = s.index_date
left join score_treatment  st  on st.sk_patient_id = s.sk_patient_id and st.index_date = s.index_date
left join score_frailty    sf  on sf.sk_patient_id = s.sk_patient_id and sf.index_date = s.index_date
left join score_activation sa  on sa.sk_patient_id = s.sk_patient_id and sa.index_date = s.index_date
left join score_coordination sco on sco.sk_patient_id = s.sk_patient_id and sco.index_date = s.index_date
left join activity         act on act.sk_patient_id = s.sk_patient_id and act.index_date = s.index_date
left join pregnancy        preg on preg.person_id = s.person_id and preg.index_date = s.index_date
left join bp_control       bp  on bp.sk_patient_id = s.sk_patient_id and bp.index_date = s.index_date
left join diabetes         dia on dia.sk_patient_id = s.sk_patient_id and dia.index_date = s.index_date
left join qrisk            qr  on qr.sk_patient_id = s.sk_patient_id and qr.index_date = s.index_date
left join asthma           am  on am.person_id = s.person_id and am.index_date = s.index_date
left join asc_service      asc_c on asc_c.sk_patient_id = s.sk_patient_id and asc_c.index_date = s.index_date
left join meds             med on med.person_id = s.person_id and med.index_date = s.index_date
left join geography        geo on geo.sk_patient_id = s.sk_patient_id and geo.index_date = s.index_date

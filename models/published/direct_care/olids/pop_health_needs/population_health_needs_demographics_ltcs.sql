{{
    config(
        materialized='table')
}}

/*
Aggregate patient counts by age, ethnicity, PCN, neighbourhood, IMD quintile, LTCs for population health needs dashboard.
Includes both registration and residence geography for comprehensive analysis.
*/

SELECT
  is_active,
  is_deceased,
  gender,
  age_band_5y,
  age_band_10y,
  age_band_nhs,
  age_band_ons,
  age_life_stage,
  ethnicity_category,
  ethnicity_subcategory,
  ethnicity_granular,
  main_language,
  pcn_name,
  neighbourhood_registered,
  borough_registered,
  neighbourhood_resident,
  borough_resident,
  icb_resident,
  practice_imd_quintile_19,
  patient_imd_quintile_19,
  has_atrial_fibrillation,
  has_coronary_heart_disease,
  has_heart_failure,
  has_hypertension,
  has_peripheral_arterial_disease,
  has_stroke_tia,
  has_asthma,
  has_copd,
  has_cyp_asthma,
  has_diabetes,
  has_gestational_diabetes,
  has_non_diabetic_hyperglycaemia,
  has_obesity,
  has_nafld,
  has_dementia,
  has_depression,
  has_severe_mental_illness,
  has_anxiety,
  has_cancer,
  has_epilepsy,
  has_parkinsons,
  has_cerebral_palsy,
  has_mnd,
  has_multiple_sclerosis,
  has_hypothyroidism,
  has_autism,
  has_adhd,
  has_familial_hypercholesterolaemia,
  has_frailty,
  has_learning_disability,
  has_learning_disability_under_14,
  has_osteoporosis,
  has_osteoarthritis,
  has_chronic_liver_disease,
  has_sickle_cell,
  has_thalassaemia,
  has_palliative_care,
  has_rheumatoid_arthritis,
  COUNT(*) AS patient_count
FROM {{ ref('population_health_needs_base') }}
GROUP BY ALL
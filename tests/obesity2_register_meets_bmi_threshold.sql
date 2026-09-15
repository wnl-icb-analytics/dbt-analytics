SELECT
    person_id,
    latest_bmi_35_date,
    latest_bmi_32_5_date,
    has_lower_bmi_threshold_ethnicity
FROM {{ ref('fct_person_obesity2_register') }}
WHERE latest_bmi_35_date IS NULL
  AND NOT (
      has_lower_bmi_threshold_ethnicity
      AND latest_bmi_32_5_date IS NOT NULL
  )

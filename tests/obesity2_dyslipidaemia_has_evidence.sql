SELECT
    person_id,
    gender,
    latest_lipid_therapy_date,
    latest_ldl_value,
    latest_triglycerides_value,
    latest_hdl_value
FROM {{ ref('fct_person_obesity2_register') }}
WHERE has_dyslipidaemia
  AND latest_lipid_therapy_date IS NULL
  AND COALESCE(latest_ldl_value < 4.1, TRUE)
  AND COALESCE(latest_triglycerides_value < 1.7, TRUE)
  AND NOT (
      gender IN ('Male', 'M')
      AND latest_hdl_value < 1.0
  )
  AND NOT (
      gender IN ('Female', 'F')
      AND latest_hdl_value < 1.3
  )

-- A BMI35 code without a measurement cannot distinguish the rollout cohorts.
SELECT COUNT(*) AS invalid_assignments
FROM {{ ref('fct_tirzepatide_cohort_status') }}
WHERE latest_bmi_value IS NULL
  AND (
      is_eligible_cohort_1 IS DISTINCT FROM FALSE
      OR is_eligible_cohort_2 IS DISTINCT FROM FALSE
      OR cohort IS DISTINCT FROM 'BMI assessment needed'
      OR bmi_category IS DISTINCT FROM 'BMI assessment needed'
  )
HAVING COUNT(*) > 0

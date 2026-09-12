-- NICE IND235 requires a recorded ACR below 70 mg/mmol in the denominator.
SELECT person_id
FROM {{ ref('fct_person_ckd_bp_ind235') }}
WHERE latest_acr_value IS NULL OR latest_acr_value >= 70

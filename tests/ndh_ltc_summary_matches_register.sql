/* The LTC NDH cohort must match the clinical NDH register in both directions. */

WITH ltc_ndh AS (
    SELECT person_id
    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE condition_code = 'NDH'
)

SELECT 'register_not_ltc' AS mismatch_direction, register.person_id
FROM {{ ref('fct_person_ndh_register') }} AS register
LEFT JOIN ltc_ndh AS ltc USING (person_id)
WHERE ltc.person_id IS NULL

UNION ALL

SELECT 'ltc_not_register' AS mismatch_direction, ltc.person_id
FROM ltc_ndh AS ltc
LEFT JOIN {{ ref('fct_person_ndh_register') }} AS register USING (person_id)
WHERE register.person_id IS NULL

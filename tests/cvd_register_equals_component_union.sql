-- CD_REG must contain exactly the union of the live CHD and stroke/TIA registers.

WITH expected_membership AS (
    SELECT person_id
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register = TRUE

    UNION

    SELECT person_id
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register = TRUE
),

actual_membership AS (
    SELECT person_id
    FROM {{ ref('fct_person_cvd_register') }}
    WHERE is_on_register = TRUE
)

SELECT
    COALESCE(expected.person_id, actual.person_id) AS person_id,
    expected.person_id IS NOT NULL AS is_expected,
    actual.person_id IS NOT NULL AS is_actual
FROM expected_membership AS expected
FULL OUTER JOIN actual_membership AS actual
    ON expected.person_id = actual.person_id
WHERE expected.person_id IS NULL OR actual.person_id IS NULL

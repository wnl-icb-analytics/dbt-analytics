-- Component flags and dates must identify every route into the composite register.

WITH component_rows AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_chd_diagnosis_date,
        NULL::DATE AS earliest_stroke_tia_diagnosis_date
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    SELECT
        person_id,
        NULL::DATE AS earliest_chd_diagnosis_date,
        earliest_diagnosis_date AS earliest_stroke_tia_diagnosis_date
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register = TRUE
),

expected AS (
    SELECT
        person_id,
        MIN(earliest_chd_diagnosis_date) AS earliest_chd_diagnosis_date,
        MIN(earliest_stroke_tia_diagnosis_date)
            AS earliest_stroke_tia_diagnosis_date
    FROM component_rows
    GROUP BY person_id
)

SELECT
    actual.person_id,
    actual.is_qualified_via_chd,
    actual.is_qualified_via_stroke_tia,
    actual.earliest_chd_diagnosis_date,
    actual.earliest_stroke_tia_diagnosis_date
FROM {{ ref('fct_person_cvd_register') }} AS actual
INNER JOIN expected
    ON actual.person_id = expected.person_id
WHERE
    actual.is_qualified_via_chd
        != (expected.earliest_chd_diagnosis_date IS NOT NULL)
    OR actual.is_qualified_via_stroke_tia
        != (expected.earliest_stroke_tia_diagnosis_date IS NOT NULL)
    OR actual.earliest_chd_diagnosis_date
        IS DISTINCT FROM expected.earliest_chd_diagnosis_date
    OR actual.earliest_stroke_tia_diagnosis_date
        IS DISTINCT FROM expected.earliest_stroke_tia_diagnosis_date

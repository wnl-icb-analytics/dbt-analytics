{% macro calculate_osteoporosis_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_osteoporosis_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates Osteoporosis register status at a given reference date.

    QOF Business Rules:
    OSTEO1_REG (Age 50-74):
    - Fragility fracture on/after 1 April 2012
    - Osteoporosis diagnosis
    - DXA confirmation (scan OR T-score ≤ -2.5)

    OSTEO2_REG (Age 75+):
    - Fragility fracture on/after 1 April 2014
    - Osteoporosis diagnosis
    - NO DXA requirement

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH osteoporosis_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_osteoporosis_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
          AND is_diagnosis_code = TRUE
    ),

    osteoporosis_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date
        FROM osteoporosis_diagnoses_filtered
        GROUP BY person_id
    ),

    -- Fragility fractures with date for age-specific filtering
    fragility_fractures_filtered AS (
        SELECT
            person_id,
            clinical_effective_date
        FROM {{ ref('int_fragility_fractures_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    fragility_person_aggregates AS (
        SELECT
            person_id,
            -- For OSTEO1_REG: fracture on/after 2012-04-01
            MAX(CASE WHEN clinical_effective_date >= '2012-04-01' THEN 1 ELSE 0 END) = 1 AS has_fracture_post_2012,
            -- For OSTEO2_REG: fracture on/after 2014-04-01
            MAX(CASE WHEN clinical_effective_date >= '2014-04-01' THEN 1 ELSE 0 END) = 1 AS has_fracture_post_2014
        FROM fragility_fractures_filtered
        GROUP BY person_id
    ),

    dxa_scans_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_dxa_scan_procedure,
            is_dxa_t_score_measurement,
            validated_t_score
        FROM {{ ref('int_dxa_scans_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    dxa_person_aggregates AS (
        SELECT
            person_id,
            MAX(CASE WHEN is_dxa_scan_procedure = TRUE THEN 1 ELSE 0 END) = 1 AS has_dxa_scan,
            MAX(CASE WHEN is_dxa_t_score_measurement = TRUE AND validated_t_score <= -2.5 THEN 1 ELSE 0 END) = 1 AS has_valid_t_score
        FROM dxa_scans_filtered
        GROUP BY person_id
    ),

    age_at_reference AS (
        SELECT
            person_id,
            birth_date_approx,
            FLOOR(DATEDIFF(
                'month',
                birth_date_approx,
                CASE
                    WHEN death_date_approx <= {{ reference_date_expr }} THEN death_date_approx
                    ELSE {{ reference_date_expr }}
                END
            ) / 12) AS age
        FROM {{ ref('dim_person_birth_death') }}
        WHERE birth_date_approx IS NOT NULL
    ),

    osteoporosis_register_logic AS (
        SELECT
            diag.person_id,
            'Osteoporosis' AS register_name,
            COALESCE(
                -- OSTEO1_REG: Age 50-74 + fracture post-2012 + DXA confirmation
                (
                    age.age BETWEEN 50 AND 74
                    AND frac.has_fracture_post_2012 = TRUE
                    AND diag.earliest_diagnosis_date IS NOT NULL
                    AND (dxa.has_dxa_scan = TRUE OR dxa.has_valid_t_score = TRUE)
                )
                OR
                -- OSTEO2_REG: Age 75+ + fracture post-2014 (no DXA required)
                (
                    age.age >= 75
                    AND frac.has_fracture_post_2014 = TRUE
                    AND diag.earliest_diagnosis_date IS NOT NULL
                ),
                FALSE
            ) AS is_on_register
        FROM osteoporosis_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
        LEFT JOIN fragility_person_aggregates frac ON diag.person_id = frac.person_id
        LEFT JOIN dxa_person_aggregates dxa ON diag.person_id = dxa.person_id
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM osteoporosis_register_logic

{% endmacro %}

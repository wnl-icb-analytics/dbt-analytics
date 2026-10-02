{% macro calculate_osteoporosis_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_osteoporosis_register.sql. Clinical evidence is bounded by the reference date. #}
    {#
    Calculates Osteoporosis register status at one or more reference dates.

    QOF v51 Business Rules:
    OSTEO1_REG (Age 50-74):
    - Fragility fracture on/after 1 April 2012
    - Osteoporosis diagnosis
    - DXA confirmation (DXA_COD OR unrounded DXA2_COD T-score <= -2.5, no lower limit)

    OSTEO2_REG (Age 75+):
    - Fragility fracture on/after 1 April 2014
    - Osteoporosis diagnosis
    - NO DXA requirement

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with a record known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    osteoporosis_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_diagnosis_code
        FROM {{ ref('int_osteoporosis_diagnoses_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
        WHERE event.is_diagnosis_code = TRUE
    ),

    osteoporosis_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date,
            MAX(clinical_effective_date) AS latest_diagnosis_date
        FROM osteoporosis_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    -- Fragility fractures with date for age-specific filtering
    fragility_fractures_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date
        FROM {{ ref('int_fragility_fractures_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    fragility_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            -- For OSTEO1_REG: fracture on/after 2012-04-01
            MAX(CASE WHEN clinical_effective_date::DATE >= '2012-04-01' THEN 1 ELSE 0 END) = 1 AS has_fracture_post_2012,
            -- For OSTEO2_REG: fracture on/after 2014-04-01
            MAX(CASE WHEN clinical_effective_date::DATE >= '2014-04-01' THEN 1 ELSE 0 END) = 1 AS has_fracture_post_2014
        FROM fragility_fractures_filtered
        GROUP BY reference_date, person_id
    ),

    dxa_scans_filtered AS (
        SELECT
            ref_date.reference_date,
            event.person_id,
            clinical_effective_date,
            is_dxa_scan_procedure,
            confirms_osteoporosis_diagnosis
        FROM {{ ref('int_dxa_scans_all') }} AS event
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('event.clinical_effective_date', 'event.date_recorded', 'ref_date.reference_date') }}
    ),

    dxa_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MAX(CASE WHEN is_dxa_scan_procedure = TRUE THEN 1 ELSE 0 END) = 1 AS has_dxa_scan,
            MAX(CASE WHEN confirms_osteoporosis_diagnosis THEN 1 ELSE 0 END) = 1 AS has_valid_t_score
        FROM dxa_scans_filtered
        GROUP BY reference_date, person_id
    ),

    age_at_reference AS (
        SELECT
            diag.reference_date,
            diag.person_id,
            FLOOR(DATEDIFF(
                'month',
                birth.birth_date_approx,
                CASE
                    WHEN birth.death_date_approx <= diag.reference_date THEN birth.death_date_approx
                    ELSE diag.reference_date
                END
            ) / 12) AS age
        FROM osteoporosis_person_aggregates AS diag
        INNER JOIN {{ ref('dim_person_birth_death') }} AS birth
            ON diag.person_id = birth.person_id
        WHERE birth.birth_date_approx IS NOT NULL
    ),

    osteoporosis_register_logic AS (
        SELECT
            diag.reference_date,
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
            ) AS is_on_register,
            diag.earliest_diagnosis_date,
            diag.latest_diagnosis_date
        FROM osteoporosis_person_aggregates diag
        LEFT JOIN age_at_reference age ON diag.person_id = age.person_id
            AND diag.reference_date = age.reference_date
        LEFT JOIN fragility_person_aggregates frac ON diag.person_id = frac.person_id
            AND diag.reference_date = frac.reference_date
        LEFT JOIN dxa_person_aggregates dxa ON diag.person_id = dxa.person_id
            AND diag.reference_date = dxa.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM osteoporosis_register_logic

{% endmacro %}

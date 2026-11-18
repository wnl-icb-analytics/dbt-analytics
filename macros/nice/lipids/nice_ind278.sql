{% macro nice_ind278(reference='current') %}
{#-
    Calculate NICE IND278 from paired CVD membership and latest lipid evidence.
    Args: reference is current or by_month.
    Returns: the IND278 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND278: https://www.nice.org.uk/indicators/ind278
-- Latest LDL or non-HDL in 12 months for secondary-prevention CVD, excluding FH and haemorrhagic stroke history.
WITH cvd AS (
    SELECT
        person_id,
        reporting_date,
        has_chd,
        has_stroke_tia,
        has_pad,
        has_familial_hypercholesterolaemia,
        has_haemorrhagic_stroke
    FROM {{ nice_ref('int_cvd_secondary_prevention_population', reference) }} AS profile
),

eligible_people AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        cvd.has_chd,
        cvd.has_stroke_tia,
        cvd.has_pad
    FROM cvd
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON cvd.person_id = population.person_id
        AND cvd.reporting_date = population.reporting_date
    -- Both exclusions apply to the whole person, even with overlapping CVD diagnoses.
    WHERE NOT cvd.has_familial_hypercholesterolaemia
        AND NOT cvd.has_haemorrhagic_stroke
),

eligible_keys AS (
    SELECT DISTINCT person_id
    FROM eligible_people
),

lipid_results AS (
    SELECT
        lipid.person_id,
        lipid.id AS observation_id,
        lipid.clinical_effective_date,
        lipid.cholesterol_value,
        lipid.is_valid_cholesterol,
        lipid.unit_status,
        lipid.recorded_value,
        lipid.source_result_unit_display,
        lipid.mapped_result_unit_display,
        lipid.conversion_factor,
        lipid.plausibility_status,
        lipid.is_lipid_review_required,
        'LDL cholesterol' AS lipid_type,
        1 AS same_day_priority,
        2.0 AS indicator_threshold
    FROM {{ ref('int_cholesterol_ldl_all') }} lipid
    INNER JOIN eligible_keys AS eligible
        ON lipid.person_id = eligible.person_id
    WHERE lipid.clinical_effective_date::DATE
        BETWEEN (SELECT MIN(DATEADD(month, -12, reporting_date)) FROM eligible_people)
            AND (SELECT MAX(reporting_date) FROM eligible_people)

    UNION ALL

    SELECT
        lipid.person_id,
        lipid.id,
        lipid.clinical_effective_date,
        lipid.cholesterol_value,
        lipid.is_valid_cholesterol,
        lipid.unit_status,
        lipid.recorded_value,
        lipid.source_result_unit_display,
        lipid.mapped_result_unit_display,
        lipid.conversion_factor,
        lipid.plausibility_status,
        lipid.is_lipid_review_required,
        'Non-HDL cholesterol',
        2,
        2.6
    FROM {{ ref('int_cholesterol_non_hdl_all') }} lipid
    INNER JOIN eligible_keys AS eligible
        ON lipid.person_id = eligible.person_id
    WHERE lipid.clinical_effective_date::DATE
        BETWEEN (SELECT MIN(DATEADD(month, -12, reporting_date)) FROM eligible_people)
            AND (SELECT MAX(reporting_date) FROM eligible_people)
),

daily_results AS (
    SELECT *
    FROM lipid_results
    -- IND278 uses the last recorded result. Invalid evidence cannot be replaced by an older success.
    -- LDL wins when LDL and non-HDL share the latest clinical day.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, clinical_effective_date::DATE
        ORDER BY same_day_priority, clinical_effective_date DESC, observation_id DESC
    ) = 1
),

last_recorded_result AS (
    SELECT
        eligible.person_id,
        eligible.reporting_date,
        result.* EXCLUDE (person_id)
    FROM eligible_people AS eligible
    ASOF JOIN daily_results AS result
        MATCH_CONDITION (eligible.reporting_date >= result.clinical_effective_date::DATE)
        ON eligible.person_id = result.person_id
)

SELECT
    eligible.person_id,
    'IND278' AS indicator_id,
    'Cardiovascular disease prevention: cholesterol treatment target (secondary prevention)' AS indicator_name,
    'The percentage of patients with cardiovascular disease (CVD) in whom the last recorded LDL or non-HDL cholesterol level (measured in the preceding 12 months) is 2.0 mmol per litre or less for LDL cholesterol or 2.6 mmol per litre or less for non-HDL cholesterol.' AS indicator_description,
    'Cardiovascular disease' AS denominator_description,
    eligible.reporting_date AS reporting_date,
    DATEADD(month, -12, eligible.reporting_date) AS measurement_period_start,
    eligible.age,
    {{ nice_practice_columns('eligible', reference) }},
    eligible.has_chd,
    eligible.has_stroke_tia,
    eligible.has_pad,
    result.observation_id AS latest_lipid_observation_id,
    result.clinical_effective_date AS latest_lipid_date,
    result.lipid_type,
    result.cholesterol_value AS latest_lipid_value,
    result.unit_status,
    result.recorded_value AS latest_lipid_recorded_value,
    result.source_result_unit_display,
    result.mapped_result_unit_display,
    result.conversion_factor,
    result.plausibility_status,
    result.is_lipid_review_required AS is_latest_lipid_review_required,
    result.indicator_threshold,
    'mmol/L' AS threshold_unit,
    TRUE AS is_in_denominator,
    result.observation_id IS NOT NULL AS is_lipid_recorded_in_last_12m,
    COALESCE(result.is_valid_cholesterol, FALSE) AS is_latest_lipid_valid,
    COALESCE(result.is_valid_cholesterol
        AND result.cholesterol_value <= result.indicator_threshold, FALSE) AS is_in_numerator,
    CASE
        WHEN result.observation_id IS NULL THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN NOT result.is_valid_cholesterol THEN 'NOT_ASSESSABLE'
        WHEN result.cholesterol_value <= result.indicator_threshold THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM eligible_people AS eligible
-- Mask every stale payload field by keeping the lower window bound on the join.
LEFT JOIN last_recorded_result AS result
    ON eligible.person_id = result.person_id
    AND eligible.reporting_date = result.reporting_date
    AND result.clinical_effective_date::DATE >= DATEADD(month, -12, eligible.reporting_date)

{% endmacro %}

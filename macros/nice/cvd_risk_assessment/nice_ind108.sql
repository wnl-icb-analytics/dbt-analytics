{#-
    Calculate NICE IND108 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND108 detail columns, one person per reporting_date.
-#}
{% macro nice_ind108(reference='current') %}
-- NICE IND108: https://www.nice.org.uk/indicators/ind108
-- QRISK2 or QRISK3 recorded within 15 months for rheumatoid arthritis members aged 30 to 84, excluding CHD, stroke, TIA and familial hypercholesterolaemia.
WITH rheumatoid_arthritis AS (
    {{ nice_register('RA', reference) }}
),
chd AS (
    {{ nice_register('CHD', reference) }}
),
stroke_tia AS (
    {{ nice_register('STIA', reference) }}
),
familial_hypercholesterolaemia AS (
    {{ nice_register('FH', reference) }}
),
population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN rheumatoid_arthritis AS ra
        ON population.person_id = ra.person_id
        AND population.reporting_date = ra.reporting_date
    LEFT JOIN chd
        ON population.person_id = chd.person_id
        AND population.reporting_date = chd.reporting_date
    LEFT JOIN stroke_tia
        ON population.person_id = stroke_tia.person_id
        AND population.reporting_date = stroke_tia.reporting_date
    LEFT JOIN familial_hypercholesterolaemia AS fh
        ON population.person_id = fh.person_id
        AND population.reporting_date = fh.reporting_date
    WHERE population.age BETWEEN 30 AND 84
        AND chd.person_id IS NULL
        AND stroke_tia.person_id IS NULL
        AND fh.person_id IS NULL
),
assessment_daily AS (
    SELECT evidence.person_id, evidence.clinical_effective_date::DATE AS event_date,
        CASE WHEN evidence.risk_score_value BETWEEN 0 AND 100 THEN evidence.risk_score_value END AS latest_risk_score
    FROM {{ ref('int_cvd_risk_assessment_all') }} AS evidence
    WHERE evidence.is_qrisk_code
        AND (evidence.concept_display ILIKE '%QRISK2%' OR evidence.concept_display ILIKE '%QRISK3%')
    -- Recording counts without a numeric result; the selected score may be null.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY evidence.person_id, evidence.clinical_effective_date::DATE
        ORDER BY evidence.clinical_effective_date DESC, evidence.id DESC
    ) = 1
),
selected AS (
    SELECT population.*, evidence.latest_risk_score,
        IFF(evidence.latest_risk_score IS NOT NULL, evidence.event_date, NULL)::DATE AS latest_risk_score_date,
        evidence.event_date AS latest_risk_assessment_date
    FROM population
    ASOF JOIN assessment_daily AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.event_date)
        ON population.person_id = evidence.person_id
),
assessed AS (
    SELECT *, COALESCE(latest_risk_assessment_date >= DATEADD(month, -15, reporting_date), FALSE) AS is_in_numerator
    FROM selected
)
SELECT
    person_id,
    'IND108' AS indicator_id,
    'Rheumatoid arthritis: cardiovascular risk assessment' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'Rheumatoid arthritis, aged 30 to 84' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}

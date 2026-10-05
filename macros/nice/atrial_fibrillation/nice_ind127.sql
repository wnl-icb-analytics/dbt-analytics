{% macro nice_ind127(reference='current') %}
{#- Calculate IND127 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND127: https://www.nice.org.uk/indicators/ind127
-- CHA2DS2-VASc assessment in 12 months, excluding a latest eligible stroke risk score of 2 or more before the period.
WITH indicator_population AS (
    SELECT
        profile.*,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_atrial_fibrillation_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    -- QOF v51 AF006 uses the latest eligible score, so a later low score replaces an older high one.
    WHERE NOT COALESCE(
        profile.latest_stroke_risk_score >= 2
        AND profile.latest_stroke_risk_score_date < DATEADD(month, -12, profile.reporting_date),
        FALSE
    )
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_chadsvasc_score,
        population.latest_chadsvasc_date,
        population.latest_chads2_score,
        population.latest_anticoagulant_order_date,
        population.latest_anticoagulant_type,
        population.latest_doac_order_date,
        population.latest_vka_order_date,
        population.is_doac_ineligible,
        population.has_doac_exception,
        population.latest_anticoagulant_review_date,
        COALESCE(population.latest_chadsvasc_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND127' AS indicator_id,
    'Atrial fibrillation: annual stroke risk assessment' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Atrial fibrillation eligible for stroke risk review' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_chadsvasc_score,
    latest_chadsvasc_date,
    latest_chads2_score,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_doac_order_date,
    latest_vka_order_date,
    is_doac_ineligible,
    has_doac_exception,
    latest_anticoagulant_review_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}

{% macro nice_ind208(reference='current') %}
{#-
    Calculate NICE IND208 at each reference date.
    Args: reference is current or by_month.
    Returns: the IND208 detail columns, one eligible person per reporting_date.
-#}
-- NICE IND208: https://www.nice.org.uk/indicators/ind208
-- Asked about falls in 12 months for people aged 65 and over with moderate or severe coded frailty.
WITH category_counts AS (
    SELECT
        person_id,
        reporting_date,
        COUNT(*) AS multimorbidity_cluster_count
    FROM {{ nice_ref('int_nice_multimorbidity_categories', reference) }} AS categories
    GROUP BY person_id, reporting_date
),

indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        COALESCE(profile.ltc_count, 0) AS ltc_count,
        COALESCE(categories.multimorbidity_cluster_count, 0) AS multimorbidity_cluster_count,
        profile.latest_frailty_severity,
        review.latest_falls_discussion_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    LEFT JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN category_counts AS categories
        ON population.person_id = categories.person_id
        AND population.reporting_date = categories.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS review
        ON population.person_id = review.person_id
        AND population.reporting_date = review.reporting_date
    WHERE (population.age >= 65
        AND profile.latest_frailty_severity IN ('Moderate', 'Severe'))
),

assessed AS (
    SELECT
        person_id,
        reporting_date,
        age,
        practice_code,
        practice_name,
        ltc_count,
        multimorbidity_cluster_count,
        latest_frailty_severity,
        CASE
            WHEN latest_falls_discussion_date BETWEEN DATEADD(month, -12, reporting_date) AND reporting_date
                THEN latest_falls_discussion_date
        END AS latest_record_date,
        COALESCE(latest_falls_discussion_date BETWEEN DATEADD(month, -12, reporting_date) AND reporting_date, FALSE) AS is_in_numerator
    FROM indicator_population
)

SELECT
    person_id,
    'IND208' AS indicator_id,
    'Multiple long-term conditions: asking about falls' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Aged 65 and over with moderate or severe frailty' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    ltc_count,
    multimorbidity_cluster_count,
    latest_frailty_severity,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}

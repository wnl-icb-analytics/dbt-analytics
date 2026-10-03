{#-
    Select measured BMI, dated ethnicity and subsequent weight advice for people aged 18 to 39.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with BMI, ethnicity and advice evidence.
-#}
{% macro calculate_nice_weight_profile(reference='current') %}
WITH population AS (
    SELECT person_id, reporting_date
    FROM ({{ nice_reference_population(reference) }})
    WHERE age BETWEEN 18 AND 39
), people AS (
    SELECT DISTINCT person_id FROM population
), bmi_daily AS (
    SELECT bmi.person_id, bmi.clinical_effective_date::DATE AS bmi_date, bmi.bmi_value
    FROM {{ ref('int_bmi_qof_all') }} AS bmi
    INNER JOIN people ON bmi.person_id = people.person_id
    WHERE bmi.source_cluster_id = 'BMIVAL_COD'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY bmi.person_id, bmi.clinical_effective_date::DATE
        ORDER BY bmi.clinical_effective_date DESC, bmi.id DESC
    ) = 1
), selected_bmi AS (
    SELECT population.person_id, population.reporting_date, bmi.bmi_date, bmi.bmi_value
    FROM population
    ASOF JOIN bmi_daily AS bmi
        MATCH_CONDITION (population.reporting_date >= bmi.bmi_date)
        ON population.person_id = bmi.person_id
), selected_ethnicity AS (
    SELECT population.person_id, population.reporting_date,
        COALESCE(demographics.ethnicity_category = 'White', FALSE) AS is_recorded_white
    FROM population
    LEFT JOIN {{ ref('dim_person_demographics_historical') }} AS demographics
        ON population.person_id = demographics.person_id
        AND demographics.effective_start_date <= population.reporting_date
        AND (demographics.effective_end_date IS NULL
            OR demographics.effective_end_date > population.reporting_date)
), advice_daily AS (
    SELECT advice.person_id, advice.clinical_effective_date::DATE AS advice_date
    FROM {{ ref('int_nice_weight_advice_all') }} AS advice
    INNER JOIN people ON advice.person_id = people.person_id
    GROUP BY advice.person_id, advice.clinical_effective_date::DATE
), advice AS (
    SELECT bmi.person_id, bmi.reporting_date, MAX(advice.advice_date) AS latest_weight_advice_date
    FROM selected_bmi AS bmi
    LEFT JOIN advice_daily AS advice
        ON bmi.person_id = advice.person_id
        AND advice.advice_date BETWEEN bmi.bmi_date AND DATEADD(day, 90, bmi.bmi_date)
        AND advice.advice_date <= bmi.reporting_date
    GROUP BY bmi.person_id, bmi.reporting_date
)
SELECT bmi.person_id, bmi.reporting_date, bmi.bmi_date, bmi.bmi_value,
    ethnicity.is_recorded_white, advice.latest_weight_advice_date
FROM selected_bmi AS bmi
INNER JOIN selected_ethnicity AS ethnicity
    ON bmi.person_id = ethnicity.person_id AND bmi.reporting_date = ethnicity.reporting_date
INNER JOIN advice
    ON bmi.person_id = advice.person_id AND bmi.reporting_date = advice.reporting_date
{% endmacro %}

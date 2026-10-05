{% macro nice_ind155(reference='current') %}
{#-
    Calculate NICE IND155 using paired SMI population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the existing detail columns.
-#}
-- NICE IND155: https://www.nice.org.uk/indicators/ind155
-- Offer of smoking cessation support in 12 months for current smokers with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.birth_date_approx,
        population.practice_code,
        population.practice_name,
        smoking.latest_smoking_status_date,
        smoking.latest_smoking_status,
        smoking.latest_smoking_intervention_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON population.person_id = smoking.person_id
        AND population.reporting_date = smoking.reporting_date
    WHERE profile.has_active_smi_diagnosis
        AND smoking.latest_smoking_status = 'Current Smoker'
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_smoking_status AS latest_smoking_status,
        population.latest_smoking_status_date AS latest_smoking_status_date,
        CASE WHEN population.latest_smoking_intervention_date >= DATEADD(month, -12, population.reporting_date) THEN population.latest_smoking_intervention_date END AS latest_record_date,
        COALESCE(population.latest_smoking_intervention_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND155' AS indicator_id,
    'Smoking: support and treatment for people with bipolar, schizophrenia and other psychoses' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Active severe mental illness, current smokers' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_smoking_status,
    latest_smoking_status_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}

{% macro nice_ind99(reference='current') %}
{#-
    Calculate NICE IND99 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND99 detail columns, one person per reporting_date.
-#}
-- NICE IND99: https://www.nice.org.uk/indicators/ind99
-- Offer of smoking cessation support recorded in 24 months for current smokers aged 15+.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.age,
        population.reporting_date AS evaluation_date,
        population.practice_code,
        population.practice_name,
        smoking.latest_smoking_status,
        smoking.latest_smoking_status_date,
        smoking.latest_never_smoked_date,
        smoking.latest_smoking_intervention_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON population.person_id = smoking.person_id
        AND population.reporting_date = smoking.reporting_date
    WHERE population.age >= 15
        AND smoking.latest_smoking_status = 'Current Smoker'
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        population.evaluation_date AS reporting_date,
        population.practice_code,
        population.practice_name,
        population.latest_smoking_status,
        population.latest_smoking_status_date,
        population.latest_never_smoked_date,
        population.latest_smoking_intervention_date,
        CASE WHEN COALESCE(population.latest_smoking_intervention_date BETWEEN DATEADD(month, -24, population.evaluation_date) AND population.evaluation_date, FALSE)
            THEN population.latest_smoking_intervention_date END AS latest_record_date,
        COALESCE(population.latest_smoking_intervention_date BETWEEN DATEADD(month, -24, population.evaluation_date) AND population.evaluation_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND99' AS indicator_id,
    'Smoking: support and treatment (all patients)' AS indicator_name,
    'The percentage of patients aged 15 years and over who are recorded as current smokers who have a record of an offer of support and treatment within the preceding 24 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -24, reporting_date) AS measurement_period_start,
    age,
    'Current smokers, aged 15 or over' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_smoking_status,
    latest_smoking_status_date,
    latest_never_smoked_date,
    latest_smoking_intervention_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed

{% endmacro %}

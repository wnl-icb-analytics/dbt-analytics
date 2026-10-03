{% macro nice_ind98(reference='current') %}
{#-
    Calculate NICE IND98 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND98 detail columns, one person per reporting_date.
-#}
-- NICE IND98: https://www.nice.org.uk/indicators/ind98
-- Offer of smoking cessation support in 12 months for current smokers with a listed LTC or SMI.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.birth_date_approx,
        population.age,
        population.reporting_date AS evaluation_date,
        population.practice_code,
        population.practice_name,
        smoking.latest_smoking_status,
        smoking.latest_smoking_status_date,
        smoking.latest_never_smoked_date,
        smoking.latest_smoking_intervention_date
    FROM {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON profile.person_id = smoking.person_id
        AND profile.reporting_date = smoking.reporting_date
    WHERE (
            profile.has_chd OR profile.has_pad OR profile.has_stroke_tia OR profile.has_hypertension
            OR profile.has_diabetes OR profile.has_copd OR profile.has_ckd OR profile.has_asthma
            OR profile.earliest_smi_diagnosis_date IS NOT NULL
        )
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
        CASE WHEN COALESCE(population.latest_smoking_intervention_date BETWEEN DATEADD(month, -12, population.evaluation_date) AND population.evaluation_date, FALSE)
            THEN population.latest_smoking_intervention_date END AS latest_record_date,
        COALESCE(population.latest_smoking_intervention_date BETWEEN DATEADD(month, -12, population.evaluation_date) AND population.evaluation_date, FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND98' AS indicator_id,
    'Smoking: support and treatment for LTC or SMI' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Current smoker with a long-term condition or SMI' AS condition_name,
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

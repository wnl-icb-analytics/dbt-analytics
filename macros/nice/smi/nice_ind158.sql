{% macro nice_ind158(reference='current') %}
{#-
    Calculate NICE IND158 using paired SMI population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the existing detail columns.
-#}
-- NICE IND158: https://www.nice.org.uk/indicators/ind158
-- Total cholesterol to HDL ratio recorded in 12 months for adults with an active SMI diagnosis, excluding cardiovascular disease diagnosed more than 12 months ago.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.birth_date_approx,
        population.practice_code,
        population.practice_name,
        physical.latest_cholesterol_hdl_ratio_date,
        profile.earliest_cvd_diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_physical_health_evidence', reference) }} AS physical
        ON population.person_id = physical.person_id
        AND population.reporting_date = physical.reporting_date
    WHERE profile.has_active_smi_diagnosis
        AND population.age >= 18
        AND NOT COALESCE(profile.earliest_cvd_diagnosis_date < DATEADD(month, -12, population.reporting_date), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_cholesterol_hdl_ratio_date AS latest_cholesterol_hdl_ratio_date,
        CASE WHEN population.latest_cholesterol_hdl_ratio_date >= DATEADD(month, -12, population.reporting_date) THEN population.latest_cholesterol_hdl_ratio_date END AS latest_record_date,
        COALESCE(population.latest_cholesterol_hdl_ratio_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND158' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: annual cholesterol' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission), aged 18 and over, without cardiovascular disease diagnosed more than 12 months ago' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_cholesterol_hdl_ratio_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}

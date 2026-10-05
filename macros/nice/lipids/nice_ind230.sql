{% macro nice_ind230(reference='current') %}
{#-
    Calculate NICE IND230 eligibility and six-month lipid-lowering treatment.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with treatment detail.
-#}
-- NICE IND230: https://www.nice.org.uk/indicators/ind230
-- Lipid-lowering therapy in the last 6 months for people on the CHD, stroke/TIA or PAD register without haemorrhagic stroke history.
WITH indicator_population AS (
    SELECT
        cvd.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_cvd_secondary_prevention_population', reference) }} AS cvd
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON cvd.person_id = population.person_id
        AND cvd.reporting_date = population.reporting_date
    WHERE NOT cvd.has_haemorrhagic_stroke
),
assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        therapy.latest_lipid_lowering_order_date,
        therapy.latest_lipid_lowering_class,
        therapy.latest_lipid_lowering_product,
        therapy.is_latest_lipid_lowering_statin,
        COALESCE(
            therapy.latest_lipid_lowering_order_date
                BETWEEN DATEADD(month, -6, population.reporting_date) AND population.reporting_date,
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_therapy_evidence', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
)

SELECT
    person_id,
    'IND230' AS indicator_id,
    'Cardiovascular disease prevention: secondary prevention with lipid lowering therapies' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Cardiovascular disease' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_lipid_lowering_order_date,
    latest_lipid_lowering_class,
    latest_lipid_lowering_product,
    is_latest_lipid_lowering_statin,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_lipid_lowering_order_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed

{% endmacro %}

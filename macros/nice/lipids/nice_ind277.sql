{% macro nice_ind277(reference='current') %}
{#-
    Calculate NICE IND277 eligibility and six-month lipid-lowering treatment.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with treatment detail.
-#}
-- NICE IND277: https://www.nice.org.uk/indicators/ind277
-- Lipid-lowering therapy in the last 6 months for people on the diabetes register with type 1 diabetes, aged over 40, without haemorrhagic stroke history.
WITH haemorrhagic_history AS (
    SELECT
        person_id,
        MIN(clinical_effective_date_raw::DATE) AS first_date,
        BOOLOR_AGG(clinical_effective_date_raw IS NULL) AS has_undated_record
    FROM {{ ref('int_haemorrhagic_stroke_diagnoses_all') }}
    GROUP BY person_id
),

indicator_population AS (
    SELECT
        diabetes.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_register('DM', reference) }}) AS diabetes
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON diabetes.person_id = population.person_id
        AND diabetes.reporting_date = population.reporting_date
    LEFT JOIN haemorrhagic_history AS haemorrhagic
        ON diabetes.person_id = haemorrhagic.person_id
    WHERE diabetes.is_on_register
        AND diabetes.diabetes_type = 'Type 1'
        AND population.age > 40
        -- Undated history excludes at every date; dated history starts on its clinical day.
        AND NOT COALESCE(
            haemorrhagic.has_undated_record
            OR haemorrhagic.first_date <= population.reporting_date,
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
    'IND277' AS indicator_id,
    'Diabetes: T1DM and lipid-lowering therapies' AS indicator_name,
    'The percentage of patients with type 1 diabetes aged over 40 years (excluding people with a history of haemorrhagic stroke) who are currently treated with a lipid-lowering therapy.' AS indicator_description,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Type 1 diabetes, aged over 40' AS denominator_description,
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

{% macro nice_ind231(reference='current') %}
{#-
    Calculate NICE IND231 eligibility and six-month lipid-lowering treatment.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with treatment detail.
-#}
-- NICE IND231: https://www.nice.org.uk/indicators/ind231
-- Lipid-lowering therapy in the last 6 months for people on the CKD register without haemorrhagic stroke history.
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
        ckd.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_register('CKD', reference) }}) AS ckd
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON ckd.person_id = population.person_id
        AND ckd.reporting_date = population.reporting_date
    LEFT JOIN haemorrhagic_history AS haemorrhagic
        ON ckd.person_id = haemorrhagic.person_id
    WHERE ckd.is_on_register
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
    'IND231' AS indicator_id,
    'Kidney conditions: CKD and lipid lowering therapies' AS indicator_name,
    'The percentage of patients with CKD, on the register, who are currently treated with a lipid-lowering therapy.' AS indicator_description,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Stage 3 to 5 kidney disease, aged 18 or over' AS denominator_description,
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

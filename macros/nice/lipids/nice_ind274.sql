{% macro nice_ind274(reference='current') %}
{#-
    Calculate NICE IND274 eligibility and six-month lipid-lowering treatment.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with treatment detail.
-#}
-- NICE IND274: https://www.nice.org.uk/indicators/ind274
-- Lipid-lowering therapy in 6 months for people aged 25 to 84 with type 2 diabetes, a CVD risk score of 10% or more recorded in the preceding 12 months, no CVD and no moderate or severe frailty.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_cvd_risk_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE population.age BETWEEN 25 AND 84
        AND profile.has_type2_diabetes
        AND profile.max_cvd_risk_score_12m >= 10
        AND NOT profile.has_cvd_including_haemorrhagic_stroke
        AND COALESCE(profile.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
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
    'IND274' AS indicator_id,
    'Diabetes: lipid-lowering therapies for primary prevention of CVD (T2DM and 10% risk)' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Type 2 diabetes with CVD risk score 10% or more' AS condition_name,
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

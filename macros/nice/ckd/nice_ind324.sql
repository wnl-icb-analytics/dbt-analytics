{% macro nice_ind324(reference='current') %}
{#-
    Calculate NICE IND324 for eligible CKD members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND324 detail columns, one person per reporting_date.
-#}
-- NICE IND324: https://www.nice.org.uk/indicators/ind324
-- SGLT2 inhibitor order in 6 months, preceded by renin-angiotensin treatment outside type 2 diabetes, for people on the CKD register with type 2 diabetes, or without it and on (or contraindicated to) ACE inhibitor or ARB therapy with eGFR 20 to 44, or eGFR 45 to 59 with ACR 22.6 or more; excludes eGFR below 20.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        profile.diabetes_type,
        profile.is_ace_inhibitor_contraindicated,
        profile.is_arb_contraindicated,
        profile.latest_acr_value,
        profile.latest_egfr_value,
        profile.latest_ras_order_before_last_sglt2_date,
        profile.latest_ras_order_date,
        profile.latest_sglt2_order_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_ckd_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE NOT COALESCE(profile.latest_egfr_value < 20, FALSE)
        AND (
            profile.diabetes_type = 'Type 2'
            OR (
                (COALESCE(profile.latest_ras_order_date >= DATEADD(month, -6, profile.reporting_date), FALSE)
                    OR (profile.is_ace_inhibitor_contraindicated AND profile.is_arb_contraindicated))
                AND (
                    profile.latest_egfr_value BETWEEN 20 AND 44
                    OR (profile.latest_egfr_value BETWEEN 45 AND 59 AND profile.latest_acr_value >= 22.6)
                )
            )
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_acr_value,
        population.latest_egfr_value,
        population.latest_sglt2_order_date AS latest_therapy_order_date,
        -- Outside type 2 diabetes NICE defines current renin-angiotensin treatment as a prescription in the last
        -- 6 months that precedes the last SGLT2 prescription (or a contraindication to both classes)
        population.diabetes_type = 'Type 2'
            OR (population.is_ace_inhibitor_contraindicated AND population.is_arb_contraindicated)
            OR population.latest_ras_order_before_last_sglt2_date >= DATEADD(month, -6, population.reporting_date) AS is_treatment_sequence_met,
        CASE
            WHEN population.latest_sglt2_order_date >= DATEADD(month, -6, population.reporting_date)
                THEN population.latest_sglt2_order_date
        END AS latest_record_date,
        COALESCE(population.latest_sglt2_order_date >= DATEADD(month, -6, population.reporting_date)
            AND (population.diabetes_type = 'Type 2'
                OR (population.is_ace_inhibitor_contraindicated AND population.is_arb_contraindicated)
                OR population.latest_ras_order_before_last_sglt2_date >= DATEADD(month, -6, population.reporting_date)), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND324' AS indicator_id,
    'Kidney conditions: CKD and SGLT2 inhibitors' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'CKD with type 2 diabetes, or eGFR 20 to 59 on renin-angiotensin therapy (with ACR 22.6 or more at eGFR 45 to 59)' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_acr_value,
    latest_egfr_value,
    latest_therapy_order_date,
    is_treatment_sequence_met,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_therapy_order_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD'
    END AS indicator_status
FROM assessed AS result

{% endmacro %}

{% macro nice_ind220(reference='current') %}
{#-
    Calculate NICE IND220 for eligible obesity register members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND220 detail columns, one person per reporting_date.
-#}
-- NICE IND220: https://www.nice.org.uk/indicators/ind220
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_bmi_date, evidence.bmi_value, evidence.bmi_source, evidence.earliest_qualifying_bmi_date,
        evidence.requires_lower_bmi_thresholds, evidence.bmi_category,
        evidence.timely_referral_date, evidence.timely_offer_date, evidence.timely_decline_date,
        evidence.latest_referral_date, evidence.latest_attendance_date, evidence.latest_end_date,
        evidence.has_previous_referral, evidence.is_currently_attending
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_weight_management_referral_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date

), assessed AS (
    SELECT population.*, GREATEST_IGNORE_NULLS(timely_referral_date, timely_offer_date, timely_decline_date) AS latest_record_date,
        latest_record_date IS NOT NULL AS is_in_numerator
    FROM population
)
SELECT person_id, 'IND220' AS indicator_id,
    'Weight management: referral to weight management programmes for obesity' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Obesity, aged 18 or over' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_bmi_date, bmi_value, bmi_source, requires_lower_bmi_thresholds, bmi_category,
    timely_referral_date, timely_offer_date, timely_decline_date,
    latest_referral_date, latest_attendance_date, latest_end_date,
    has_previous_referral, is_currently_attending,
    latest_record_date, TRUE AS is_in_denominator, is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
-- Timely achievement overrides programme exclusions after the earliest follow-up window closes.
WHERE DATEADD(day, 90, earliest_qualifying_bmi_date) <= reporting_date
    AND (is_in_numerator OR (NOT is_currently_attending AND NOT has_previous_referral))
{% endmacro %}

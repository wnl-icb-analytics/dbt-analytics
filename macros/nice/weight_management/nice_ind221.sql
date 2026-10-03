{% macro nice_ind221(reference='current') %}
{#-
    Calculate NICE IND221 for eligible obesity register members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND221 detail columns, one person per reporting_date.
-#}
-- NICE IND221: https://www.nice.org.uk/indicators/ind221
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_bmi_date, evidence.bmi_value, evidence.bmi_source,
        evidence.requires_lower_bmi_thresholds, evidence.bmi_category,
        evidence.timely_referral_date, evidence.timely_offer_date, evidence.timely_decline_date,
        evidence.latest_referral_date, evidence.latest_attendance_date, evidence.latest_end_date,
        evidence.has_previous_referral, evidence.is_currently_attending,
        hypertension.person_id IS NOT NULL AS has_hypertension, diabetes.person_id IS NOT NULL AS has_diabetes
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_weight_management_referral_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    LEFT JOIN ({{ nice_register('HTN', reference) }}) AS hypertension
        ON population.person_id = hypertension.person_id AND population.reporting_date = hypertension.reporting_date
    LEFT JOIN ({{ nice_register('DM', reference) }}) AS diabetes
        ON population.person_id = diabetes.person_id AND population.reporting_date = diabetes.reporting_date
    WHERE hypertension.person_id IS NOT NULL OR diabetes.person_id IS NOT NULL
), assessed AS (
    SELECT population.*, timely_referral_date AS latest_record_date,
        latest_record_date IS NOT NULL AS is_in_numerator
    FROM population
)
SELECT person_id, 'IND221' AS indicator_id,
    'Weight management referral with hypertension or diabetes' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Obesity with hypertension or diabetes' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_bmi_date, bmi_value, bmi_source, requires_lower_bmi_thresholds, bmi_category,
    timely_referral_date, timely_offer_date, timely_decline_date,
    latest_referral_date, latest_attendance_date, latest_end_date,
    has_previous_referral, is_currently_attending, has_hypertension, has_diabetes,
    latest_record_date, TRUE AS is_in_denominator, is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
-- Achievement precedes exclusions, so timely referrals and programme starts cannot remove achievers.
WHERE is_in_numerator OR (NOT is_currently_attending)
{% endmacro %}

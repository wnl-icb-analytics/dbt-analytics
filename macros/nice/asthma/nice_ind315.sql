{#-
    Calculate NICE IND315 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND315 detail columns, one person per reporting_date.
-#}
{% macro nice_ind315(reference='current') %}
-- NICE IND315: https://www.nice.org.uk/indicators/ind315
-- Annual asthma review for members aged 6 or over with high SABA use, repeated steroid courses or GP-coded admissions in the preceding risk year.
WITH reviews AS (
    {{ nice_ind273(reference) }}
)
SELECT r.person_id, 'IND315' AS indicator_id,
    'Asthma: annual review (higher risk patients)' AS indicator_name,
    r.reporting_date, r.measurement_period_start, r.age,
    'Asthma (aged 6 and over, higher risk)' AS condition_name,
    {{ nice_practice_columns(none, reference) }},
    risk.risk_period_start, risk.risk_period_end, risk.saba_inhaler_count,
    risk.oral_steroid_course_count, risk.latest_asthma_admission_date,
    risk.has_high_saba_use, risk.has_repeated_oral_steroids, risk.has_asthma_admission,
    r.latest_review_date, r.latest_record_date,
    TRUE AS is_in_denominator, r.is_in_numerator, r.indicator_status
FROM reviews r
INNER JOIN {{ nice_ref('int_nice_asthma_risk', reference) }} risk
    ON r.person_id = risk.person_id AND r.reporting_date = risk.reporting_date
WHERE r.age >= 6 AND risk.is_higher_risk
{% endmacro %}

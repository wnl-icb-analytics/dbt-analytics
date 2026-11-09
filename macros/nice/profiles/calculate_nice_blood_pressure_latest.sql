{% macro calculate_nice_blood_pressure_latest(reference='current') %}
{#-
    Select latest complete NICE BP evidence for the six BP control registers.
    Args: reference is current or by_month.
    Returns: the eight existing BP fields plus reporting_date, one person/date
             with complete evidence among active, living, non-test register members.
-#}
-- NICE control evidence: latest complete pair through the reference date.
WITH bp_registers AS (
    {% for condition in ['HTN', 'CHD', 'STIA', 'PAD', 'CKD', 'DM'] %}
    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_register(condition, reference) }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

bp_people AS (
    SELECT DISTINCT
        person_id,
        reporting_date
    FROM bp_registers
),

candidates AS (
    SELECT
        people.person_id,
        people.reporting_date
    FROM bp_people AS people
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON people.person_id = population.person_id
        AND people.reporting_date = population.reporting_date
)

SELECT
    candidate.person_id,
    pair.reading_date AS latest_bp_date,
    pair.systolic_value AS latest_systolic_value,
    pair.diastolic_value AS latest_diastolic_value,
    pair.is_valid_bp,
    pair.is_home_bp_event,
    pair.is_abpm_bp_event,
    pair.applied_measurement_context,
    candidate.reporting_date
FROM candidates AS candidate
-- Daily selection retains invalid-only days; do not fall back to an older valid pair.
ASOF JOIN {{ ref('int_nice_blood_pressure_all') }} AS pair
    MATCH_CONDITION (candidate.reporting_date >= pair.reading_date)
    ON candidate.person_id = pair.person_id
-- A missing pair remains a missing input when indicators left join this evidence.
WHERE pair.reading_date IS NOT NULL
{% endmacro %}

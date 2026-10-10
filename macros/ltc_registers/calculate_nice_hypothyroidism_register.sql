{% macro calculate_nice_hypothyroidism_register(reference='current', reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
{#-
    Calculate NICE IND138 membership from hypothyroidism, treatment and latest diagnosis.
    Args: reference selects the population; reference_dates supplies reference_date rows.
    Returns: one treated, non-subclinical register member per person and reference_date.
-#}
-- NICE IND138: https://www.nice.org.uk/indicators/ind138
WITH register_dates AS (
    {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
), thyroid_register AS (
    {{ calculate_hypothyroidism_register(reference_dates='SELECT reference_date FROM register_dates') }}
), population AS (
    SELECT population.person_id, dates.reference_date, population.age, population.practice_code,
        thyroid.earliest_diagnosis_date, thyroid.latest_diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN register_dates AS dates ON population.reporting_date = dates.reference_date
    INNER JOIN thyroid_register AS thyroid ON population.person_id = thyroid.person_id
        AND dates.reference_date = thyroid.reference_date
    WHERE thyroid.is_on_register
), thyroid_events AS (
    SELECT person_id, id, clinical_effective_date, date_recorded, FALSE AS is_subclinical
    FROM {{ ref('int_hypothyroidism_diagnoses_all') }}
    UNION ALL
    SELECT person_id, id, clinical_effective_date, date_recorded, TRUE AS is_subclinical
    FROM {{ ref('int_subclinical_hypothyroidism_diagnoses_all') }}
), thyroid_records AS (
    -- A diagnosis can belong to both lists. Keep its subclinical classification.
    SELECT person_id, clinical_effective_date,
        BOOLOR_AGG(is_subclinical) AS is_subclinical,
        {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date,
        TO_VARCHAR(clinical_effective_date, 'YYYY-MM-DD HH24:MI:SS.FF9')
            || '|' || TO_VARCHAR(id) AS record_key
    FROM thyroid_events
    GROUP BY person_id, id, clinical_effective_date, date_recorded
), latest_thyroid AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, known_date FROM thyroid_records', 'register_dates') }}
), treatment AS (
    SELECT population.person_id, population.reference_date,
        MAX(medication.order_date::DATE) AS latest_levothyroxine_order_date
    FROM population
    INNER JOIN {{ ref('int_levothyroxine_medications_all') }} AS medication
        ON population.person_id = medication.person_id
        AND medication.order_date::DATE > DATEADD(month, -6, population.reference_date)
        AND {{ ltc_register_known_by('medication.order_date', 'medication.date_recorded', 'population.reference_date') }}
    GROUP BY population.person_id, population.reference_date
)
SELECT population.person_id, population.reference_date, population.age, population.practice_code,
    TRUE AS is_on_register, population.earliest_diagnosis_date, population.latest_diagnosis_date,
    record.clinical_effective_date AS latest_thyroid_diagnosis_date,
    treatment.latest_levothyroxine_order_date
FROM population
INNER JOIN latest_thyroid AS latest ON population.person_id = latest.person_id
    AND population.reference_date = latest.reference_date
INNER JOIN thyroid_records AS record ON latest.person_id = record.person_id
    AND latest.record_key = record.record_key
INNER JOIN treatment ON population.person_id = treatment.person_id
    AND population.reference_date = treatment.reference_date
WHERE NOT record.is_subclinical
{% endmacro %}

{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = calculate_nice_cervical_screening_evidence('by_month') %}
{% set replacements = {
    'int_nice_reference_population_by_month': 'synthetic_population',
    'int_cervical_screening_all': 'synthetic_screening',
    'int_pregnancy_observations_all': 'synthetic_pregnancy',
    'int_cervix_removal_all': 'synthetic_removal'
} %}
{% set result = namespace(sql=calculation) %}
{% for model, fixture in replacements.items() %}
    {% set result.sql = result.sql | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_candidates AS (
    SELECT
        -9601::NUMBER AS person_id,
        column1::DATE AS reporting_date,
        35 AS age,
        'Female' AS gender
    FROM VALUES ('2024-03-31'), ('2024-04-01'), ('2024-04-30'), ('2025-01-31'), ('2027-12-31')
    UNION ALL
    SELECT -9602::NUMBER, '2024-03-31'::DATE, 24, 'Female'
    UNION ALL
    SELECT -9602::NUMBER, '2024-04-30'::DATE, 25, 'Female'
    UNION ALL
    SELECT -9603::NUMBER, '2024-04-30'::DATE, 35, 'Male'
    UNION ALL
    SELECT -9604::NUMBER, column1::DATE, 35, 'Female'
    FROM VALUES ('2025-01-15'), ('2025-01-16')
),
synthetic_population AS (
    SELECT
        synthetic_candidates.*,
        '1989-01-01'::DATE AS birth_date_approx,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM synthetic_candidates
),
synthetic_screening AS (
    SELECT
        -9601::NUMBER AS person_id,
        column1::TIMESTAMP_NTZ AS clinical_effective_date,
        column2::BOOLEAN AS is_completed_screening,
        column3::BOOLEAN AS is_unsuitable_screening,
        column4::BOOLEAN AS is_declined_screening,
        column5::BOOLEAN AS is_non_response_screening
    FROM VALUES
        ('2020-10-01', TRUE, FALSE, FALSE, FALSE),
        ('2024-04-01', FALSE, TRUE, FALSE, FALSE),
        ('2024-04-02', TRUE, FALSE, FALSE, FALSE),
        ('2024-04-20', FALSE, FALSE, TRUE, FALSE),
        ('2024-04-21', FALSE, FALSE, FALSE, TRUE)
),
synthetic_pregnancy AS (
    SELECT
        -9601::NUMBER AS person_id,
        column1::TIMESTAMP_NTZ AS clinical_effective_date,
        column2::BOOLEAN AS is_pregnancy_code,
        column3::BOOLEAN AS is_delivery_outcome_code
    FROM VALUES
        ('2024-03-31 00:00:00', TRUE, FALSE),
        ('2024-03-31 12:00:00', TRUE, FALSE),
        ('2024-04-30 00:00:00', TRUE, FALSE),
        ('2024-04-30 00:00:00', FALSE, TRUE)
    UNION ALL
    SELECT -9604::NUMBER, '2024-04-15 00:00:00'::TIMESTAMP_NTZ, TRUE, FALSE
),
synthetic_removal AS (
    SELECT
        -9601::NUMBER AS person_id,
        '2024-04-30'::TIMESTAMP_NTZ AS clinical_effective_date
),
actual AS ({{ result.sql }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 8
    OR COUNT_IF(person_id = -9601 AND reporting_date = '2024-03-31'
        AND latest_completed_date = '2020-10-01' AND programme_status = 'Up to Date'
        AND latest_pregnancy_date = '2024-03-31 00:00:00' AND is_currently_pregnant
        AND first_cervix_removal_date IS NULL AND latest_non_response_date IS NULL) <> 1
    OR COUNT_IF(person_id = -9601 AND reporting_date = '2024-04-01'
        AND latest_completed_date = '2020-10-01' AND latest_screening_date = '2024-04-01'
        AND latest_unsuitable_date = '2024-04-01' AND programme_status = 'Unsuitable') <> 1
    OR COUNT_IF(person_id = -9601 AND reporting_date = '2024-04-30'
        AND latest_completed_date = '2024-04-02' AND latest_screening_date = '2024-04-21'
        AND latest_non_response_date = '2024-04-21' AND programme_status = 'Up to Date'
        AND first_cervix_removal_date = '2024-04-30' AND NOT is_currently_pregnant) <> 1
    OR COUNT_IF(person_id = -9601 AND reporting_date = '2025-01-31'
        AND latest_completed_date = '2024-04-02' AND NOT is_currently_pregnant) <> 1
    OR COUNT_IF(person_id = -9601 AND reporting_date = '2027-12-31'
        AND programme_status = 'Overdue') <> 1
    OR COUNT_IF(person_id = -9602 AND reporting_date = '2024-04-30'
        AND programme_status = 'Never Screened' AND latest_completed_date IS NULL
        AND NOT is_currently_pregnant) <> 1
    OR COUNT_IF(person_id = -9604 AND reporting_date = '2025-01-15'
        AND latest_pregnancy_date = '2024-04-15 00:00:00'
        AND latest_delivery_date IS NULL AND is_currently_pregnant) <> 1
    OR COUNT_IF(person_id = -9604 AND reporting_date = '2025-01-16'
        AND latest_pregnancy_date = '2024-04-15 00:00:00'
        AND latest_delivery_date IS NULL AND NOT is_currently_pregnant) <> 1

{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=calculate_nice_hba1c_evidence('by_month')) %}
{% for condition in ['DM', 'NDH', 'GESTDIAB', 'SMI'] %}
    {% set query.sql = query.sql | replace(nice_register(condition, 'by_month') | string, 'SELECT person_id, reporting_date FROM synthetic_population') %}
{% endfor %}
{% set query.sql = query.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% for model, fixture in {'int_hba1c_all': 'synthetic_hba1c', 'int_fructosamine_all': 'synthetic_fructosamine', 'int_diabetes_max_tolerated_treatment_all': 'synthetic_dmmax'}.items() %}
    {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        -9201::NUMBER AS person_id,
        column1::DATE AS reporting_date,
        30 AS age
    FROM VALUES ('2024-01-31')
),
synthetic_hba1c AS (
    SELECT
        -9201::NUMBER AS person_id,
        column1::VARCHAR AS id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::FLOAT AS hba1c_ifcc,
        column4::BOOLEAN AS is_valid_hba1c
    FROM VALUES ('SYN_010', '2024-01-31 08:00:00', 48, TRUE),
        ('SYN_020', '2024-01-31 09:00:00', 52, TRUE),
        ('SYN_999', '2024-01-31 12:00:00', NULL, FALSE)
),
synthetic_fructosamine AS (
    SELECT person_id, clinical_effective_date FROM synthetic_hba1c WHERE FALSE
),
synthetic_dmmax AS (
    SELECT * FROM synthetic_fructosamine
),
actual AS ({{ query.sql }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 1
    OR COALESCE(COUNT_IF(latest_hba1c_observation_id = 'SYN_020'
        AND latest_hba1c_value = 52 AND is_latest_hba1c_valid), 0) <> 1

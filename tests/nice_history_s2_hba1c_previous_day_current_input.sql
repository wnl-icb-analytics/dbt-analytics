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
    FROM VALUES (DATEADD(day, -1, CURRENT_DATE()))
),
synthetic_hba1c AS (
    SELECT
        -9201::NUMBER AS person_id,
        'SYN_HBA1C'::VARCHAR AS id,
        DATEADD(day, -2, CURRENT_DATE())::TIMESTAMP_NTZ AS clinical_effective_date,
        48::FLOAT AS hba1c_ifcc,
        TRUE AS is_valid_hba1c
),
synthetic_fructosamine AS (
    SELECT person_id, clinical_effective_date FROM synthetic_hba1c WHERE FALSE
),
synthetic_dmmax AS (
    SELECT * FROM synthetic_fructosamine
),
stored_profile AS ({{ query.sql }}),
current_population AS (
    SELECT -9201::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date
),
expected AS (
    SELECT * REPLACE (CURRENT_DATE()::DATE AS reporting_date) FROM stored_profile
),
actual AS (
    SELECT profile.*
    FROM {{ nice_ref('int_nice_hba1c_evidence', 'current') | replace(ref('int_nice_hba1c_evidence') | string, 'stored_profile') }} AS profile
    INNER JOIN current_population AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
),
differences AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
)
SELECT COUNT(*) AS rows_total
FROM differences
HAVING COUNT(*) <> 0
    OR (SELECT COUNT(*) FROM actual) <> 1

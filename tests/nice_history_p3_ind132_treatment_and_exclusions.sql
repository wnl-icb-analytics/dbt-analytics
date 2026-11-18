{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=nice_ind132('by_month')) %}
{% set query.sql = query.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query.sql = query.sql | replace(nice_register('CHD', 'by_month') | string, 'SELECT person_id, reporting_date FROM synthetic_population') %}
{% set query.sql = query.sql | replace(nice_ref('int_nice_therapy_evidence', 'by_month') | string, 'synthetic_therapy') %}
{% set query.sql = query.sql | replace(ref('int_antithrombotic_treatment_all') | string, 'synthetic_records') %}
{% set query.sql = query.sql | replace(ref('int_antithrombotic_contraindication_all') | string, 'synthetic_contraindications') %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        50 AS age,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM VALUES (1), (2), (3), (4) AS people
    CROSS JOIN (SELECT column1 FROM VALUES ('2024-01-31'), ('2024-02-01')) AS dates
),

synthetic_therapy AS (
    SELECT
        person_id,
        reporting_date,
        IFF(person_id = 1, '2023-01-31'::DATE, NULL::DATE) AS latest_antiplatelet_order_date,
        IFF(person_id = 2, '2023-01-31'::DATE, NULL::DATE) AS latest_anticoagulant_order_date,
        IFF(person_id = 2, 'DOAC', NULL)::VARCHAR AS latest_anticoagulant_type
    FROM synthetic_population
),

synthetic_records AS (
    SELECT
        1::NUMBER AS person_id,
        'OSAL_COD'::VARCHAR AS source_cluster_id,
        '2024-02-02'::TIMESTAMP_NTZ AS clinical_effective_date
),

synthetic_contraindications AS (
    SELECT
        2::NUMBER AS person_id,
        column1::VARCHAR AS drug_class,
        '2023-01-31'::TIMESTAMP_NTZ AS clinical_effective_date,
        FALSE AS is_persisting
    FROM VALUES ('SALICYLATE'), ('CLOPIDOGREL'), ('ORAL_ANTICOAGULANT'), ('DIPYRIDAMOLE')
    UNION ALL
    SELECT 3, column1::VARCHAR, '2024-02-01'::TIMESTAMP_NTZ, TRUE
    FROM VALUES ('SALICYLATE'), ('CLOPIDOGREL'), ('ORAL_ANTICOAGULANT'), ('DIPYRIDAMOLE')
),

actual AS ({{ query.sql }}),

expected AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_status
    FROM VALUES (1, '2024-01-31', 'ACHIEVED'),
        (1, '2024-02-01', 'NOT_TREATED_IN_PERIOD'),
        (2, '2024-02-01', 'NOT_TREATED_IN_PERIOD'),
        (3, '2024-01-31', 'NEVER_TREATED'),
        (4, '2024-01-31', 'NEVER_TREATED'),
        (4, '2024-02-01', 'NEVER_TREATED')
),

actual_occurrences AS (
    SELECT person_id, reporting_date, indicator_status, COUNT(*) AS occurrences
    FROM actual
    GROUP BY ALL
),

expected_occurrences AS (
    SELECT person_id, reporting_date, indicator_status, COUNT(*) AS occurrences
    FROM expected
    GROUP BY ALL
),

failures AS (
    (SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
    UNION ALL
    (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences)
)
SELECT COUNT(*) AS failure_count
FROM failures
HAVING COUNT(*) > 0

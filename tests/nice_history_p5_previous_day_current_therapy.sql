{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind134('current') %}
{% set calculation = calculation | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(nice_register('DM', 'current') | string, 'SELECT * FROM synthetic_register') %}
{% set calculation = calculation | replace(ref('int_proteinuria_all') | string, 'synthetic_proteinuria') %}
{% set calculation = calculation | replace(ref('int_ras_contraindication_all') | string, 'synthetic_contra') %}
{% set calculation = calculation | replace(ref('int_nice_therapy_evidence') | string, 'synthetic_therapy') %}

WITH
synthetic_population AS (
    SELECT -9517::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date,
        40 AS age, 'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
),
synthetic_register AS (
    SELECT person_id, reporting_date
    FROM synthetic_population
),
synthetic_proteinuria AS (
    SELECT -9517::NUMBER AS person_id, '2020-01-01'::DATE AS clinical_effective_date,
        'PRT_COD' AS source_cluster_id
),
synthetic_contra AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS clinical_effective_date,
        NULL::VARCHAR AS drug_class, FALSE AS is_persisting
    WHERE FALSE
),
synthetic_therapy AS (
    SELECT -9517::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS latest_ras_order_date,
        'ACE_INHIBITOR' AS latest_ras_class
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 1 OR COUNT_IF(is_in_numerator
    AND reporting_date = CURRENT_DATE() AND latest_therapy_order_date = DATEADD(day, -1, CURRENT_DATE())) <> 1

{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = calculate_nice_immunosuppression('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_immunosuppression_all') | string, 'synthetic_evidence') %}
{% set calculation = calculation | replace(covid_autumn_config() | string,
    "SELECT '2021-04-30'::DATE AS audit_end_date") %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        people.column2::DATE AS birth_date_approx,
        dates.column1::DATE AS reporting_date,
        75 AS age,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES
        (-9621, '1945-09-30'),
        (-9622, '1945-09-30'),
        (-9623, '1945-09-30'),
        (-9624, '1945-09-30'),
        (-9625, '1945-04-30'),
        (-9626, '1946-04-30') AS people
    CROSS JOIN (VALUES ('2021-03-31'), ('2021-04-30'), ('2021-05-31')) AS dates
),
synthetic_evidence AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS evidence_date,
        column3::VARCHAR AS evidence_type
    FROM VALUES
        (-9621, '2020-09-30', 'IMMRX_COD'),
        (-9621, '2021-04-30 12:00:00', 'IMMRX_COD'),
        (-9622, '2021-04-01', 'IMMDX_COV_COD'),
        (-9623, '2018-03-31', 'IMM_ADM_COD'),
        (-9624, '2021-03-31 00:00:00', 'DXT_CHEMO_COD'),
        (-9624, '2021-03-31 12:00:00', 'DXT_CHEMO_COD'),
        (-9625, '2000-01-01', 'IMMDX_COV_COD'),
        (-9626, '2000-01-01', 'IMMDX_COV_COD')
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 10
    OR COUNT_IF(reporting_date = '2021-03-31') <> 4
    OR COUNT_IF(reporting_date = '2021-04-30') <> 3
    OR COUNT_IF(reporting_date = '2021-05-31') <> 3
    OR COUNT_IF(person_id = -9621 AND reporting_date = '2021-03-31'
        AND latest_evidence_date = '2020-09-30') <> 1
    OR COUNT_IF(person_id = -9623 AND reporting_date = '2021-03-31'
        AND latest_evidence_date = '2018-03-31') <> 1
    OR COUNT_IF(person_id = -9624 AND reporting_date = '2021-03-31'
        AND latest_evidence_date = '2021-03-31 00:00:00') <> 1
    OR COUNT_IF(person_id = -9624 AND reporting_date = '2021-04-30'
        AND latest_evidence_date = '2021-03-31 12:00:00') <> 1
    OR COUNT_IF(person_id = -9621 AND reporting_date > '2021-03-31') <> 0
    OR COUNT_IF(person_id = -9625 AND reporting_date > '2021-03-31') <> 0
    OR COUNT_IF(person_id = -9626 AND reporting_date = '2021-03-31') <> 0

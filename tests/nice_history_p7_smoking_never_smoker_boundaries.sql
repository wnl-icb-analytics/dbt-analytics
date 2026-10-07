{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculations = [] %}
{% for indicator in [97, 156] %}
    {% set calculation = nice_ind97('by_month') if indicator == 97 else nice_ind156('by_month') %}
    {% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
    {% set calculation = calculation | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
    {% set calculation = calculation | replace(ref('int_nice_smoking_evidence_by_month') | string, 'synthetic_smoking') %}
    {% do calculations.append(calculation) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        person.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        25 AS age,
        '2000-04-15'::DATE AS birth_date_approx,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-7701), (-7702), (-7703) AS person
    CROSS JOIN (VALUES ('2026-03-31'), ('2026-04-30')) AS dates
),
synthetic_ltc AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        TRUE AS has_chd,
        FALSE AS has_pad,
        FALSE AS has_stroke_tia,
        FALSE AS has_hypertension,
        FALSE AS has_diabetes,
        FALSE AS has_copd,
        FALSE AS has_ckd,
        FALSE AS has_asthma,
        NULL::DATE AS earliest_smi_diagnosis_date,
        IFF(person_id = -7703, '2025-04-16', '2020-01-01')::DATE AS earliest_smoking_ltc_diagnosis_date,
        earliest_smoking_ltc_diagnosis_date AS earliest_smoking_smi_ltc_diagnosis_date
    FROM synthetic_population
),
synthetic_smoking AS (
    SELECT
        person_id,
        reporting_date,
        'Never Smoked' AS latest_smoking_status,
        IFF(person_id = -7702, '2025-04-15', '2025-04-16')::DATE AS latest_smoking_status_date,
        latest_smoking_status_date AS latest_never_smoked_date,
        NULL::DATE AS latest_smoking_unsuitable_date,
        NULL::DATE AS latest_smoking_intervention_date
    FROM synthetic_population
),
actual AS (
    {% for calculation in calculations %}
    SELECT * FROM ({{ calculation }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 12
    OR COUNT_IF(reporting_date = '2026-03-31' AND is_never_smoker_covered) <> 0
    OR COUNT_IF(reporting_date = '2026-03-31' AND NOT is_in_numerator) <> 0
    OR COUNT_IF(reporting_date = '2026-04-30' AND person_id = -7701
        AND is_never_smoker_covered AND is_in_numerator
        AND NOT is_status_recorded_in_period AND latest_record_date IS NULL) <> 2
    OR COUNT_IF(reporting_date = '2026-04-30' AND person_id IN (-7702, -7703)
        AND NOT is_never_smoker_covered AND NOT is_in_numerator) <> 4

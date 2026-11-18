{{ config(tags=['monthly-full', 'nice-history']) }}

{% set childhood = nice_ind215('current') %}
{% set cvd = nice_ind278('current') %}
{% set replacements = {
    'int_nice_childhood_immunisation_profile': 'synthetic_childhood_profile',
    'int_cvd_secondary_prevention_population': 'synthetic_cvd_profile',
    'dim_person_active_patients': 'synthetic_active',
    'dim_person_age': 'synthetic_age',
    'dim_person_demographics': 'synthetic_demographics',
    'int_cholesterol_ldl_all': 'synthetic_ldl',
    'int_cholesterol_non_hdl_all': 'synthetic_non_hdl'
} %}
{% set calculations = namespace(childhood=childhood, cvd=cvd) %}
{% for model, fixture in replacements.items() %}
    {% set calculations.childhood = calculations.childhood | replace(ref(model) | string, fixture) %}
    {% set calculations.cvd = calculations.cvd | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_active AS (
    SELECT column1::NUMBER AS person_id, 'SYNTHETIC'::VARCHAR AS current_practice_code,
        'Synthetic practice'::VARCHAR AS current_practice_name
    FROM VALUES (-9020), (-9021)
),
synthetic_age AS (
    SELECT -9020::NUMBER AS person_id, 0::NUMBER AS age,
        DATEADD(month, -8, CURRENT_DATE())::DATE AS birth_date_approx
    UNION ALL
    SELECT -9021::NUMBER, 30::NUMBER, DATEADD(year, -30, CURRENT_DATE())::DATE
),
synthetic_demographics AS (
    SELECT person_id, 'Female'::VARCHAR AS gender
    FROM synthetic_active
),
synthetic_childhood_profile AS (
    SELECT -9020::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        DATEADD(month, -8, CURRENT_DATE())::DATE AS birth_date_approx,
        3::NUMBER(18,0) AS dtap_doses_by_8_months, FALSE AS has_dtap_contraindication
),
synthetic_cvd_profile AS (
    SELECT -9021::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        TRUE AS has_chd, FALSE AS has_stroke_tia, FALSE AS has_pad,
        FALSE AS has_familial_hypercholesterolaemia, FALSE AS has_haemorrhagic_stroke
),
synthetic_ldl AS (
    SELECT -9021::NUMBER AS person_id, 'SYN_LIPID'::VARCHAR AS id,
        DATEADD(day, -10, CURRENT_DATE())::TIMESTAMP_NTZ AS clinical_effective_date,
        1.5::FLOAT AS cholesterol_value, TRUE AS is_valid_cholesterol,
        'STANDARD'::VARCHAR AS unit_status, 1.5::FLOAT AS recorded_value,
        'mmol/L'::VARCHAR AS source_result_unit_display, 'mmol/L'::VARCHAR AS mapped_result_unit_display,
        1::FLOAT AS conversion_factor, 'PLAUSIBLE'::VARCHAR AS plausibility_status,
        FALSE AS is_lipid_review_required
),
synthetic_non_hdl AS (
    SELECT * FROM synthetic_ldl WHERE FALSE
),
fixture_childhood_result AS ({{ calculations.childhood }}),
fixture_cvd_result AS ({{ calculations.cvd }})
SELECT 'IND215' AS model, COUNT(*) AS rows_total
FROM fixture_childhood_result
HAVING COUNT(*) <> 1 OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 1
UNION ALL
SELECT 'IND278', COUNT(*)
FROM fixture_cvd_result
HAVING COUNT(*) <> 1 OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 1

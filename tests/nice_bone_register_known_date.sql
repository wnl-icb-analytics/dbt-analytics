{{ config(tags=['monthly-full', 'nice-history']) }}
{% set ns = namespace(calculation=calculate_osteoporosis_register(reference_dates="SELECT column1::DATE AS reference_date FROM VALUES ('2026-09-30'), ('2026-10-31')")) %}
{% for model, fixture in [
 ('int_osteoporosis_diagnoses_all', 'synthetic_diagnoses'),
 ('int_fragility_fractures_all', 'synthetic_fractures'),
 ('int_dxa_scans_all', 'synthetic_dxa'),
 ('dim_person_birth_death', 'synthetic_birth')
] %}
 {% set ns.calculation = ns.calculation | replace(ref(model) | string, fixture) %}
{% endfor %}
WITH synthetic_diagnoses AS (
 SELECT -541::NUMBER AS person_id, '2026-01-01'::DATE AS clinical_effective_date,
     '2026-10-01'::DATE AS date_recorded, TRUE AS is_diagnosis_code
), synthetic_fractures AS (
 SELECT -541::NUMBER AS person_id, '2012-04-01'::DATE AS clinical_effective_date,
     '2012-04-01'::DATE AS date_recorded
), synthetic_dxa AS (
 SELECT -541::NUMBER AS person_id, '2026-01-01'::DATE AS clinical_effective_date,
     '2026-01-01'::DATE AS date_recorded, TRUE AS is_dxa_scan_procedure,
     FALSE AS confirms_osteoporosis_diagnosis
), synthetic_birth AS (
 SELECT -541::NUMBER AS person_id, '1966-01-15'::DATE AS birth_date_approx,
     NULL::DATE AS death_date_approx
), actual AS ({{ ns.calculation }})
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 1
    OR COUNT_IF(reference_date = '2026-10-31' AND is_on_register) <> 1

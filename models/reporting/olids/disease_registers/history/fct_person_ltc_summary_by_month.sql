{{
    config(
        materialized='table',
        cluster_by=['month_end_date', 'person_id'],
        tags=['monthly-full'])
}}

-- Long-term condition registers at each completed month-end, in the shape of
-- fct_person_ltc_summary. Condition metadata comes from ltc_register_denominator_rules.

{% set register_models = ltc_register_history_models() %}

WITH condition_union AS (
    {% for condition_code, register_model in register_models %}
    SELECT
        person_id,
        month_end_date,
        practice_code,
        '{{ condition_code }}' AS condition_code,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM {{ ref(register_model) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

condition_metadata AS (
    SELECT
        UPPER(condition_code) AS condition_code,
        condition_name,
        clinical_domain,
        CAST(is_qof AS BOOLEAN) AS is_qof
    FROM {{ ref('ltc_register_denominator_rules') }}
)

SELECT
    cu.person_id,
    cu.month_end_date,
    cu.practice_code,
    cu.condition_code,
    cm.condition_name,
    cm.clinical_domain,
    cm.is_qof,
    cu.earliest_diagnosis_date,
    cu.latest_diagnosis_date
FROM condition_union AS cu
LEFT JOIN condition_metadata AS cm
    ON cu.condition_code = cm.condition_code

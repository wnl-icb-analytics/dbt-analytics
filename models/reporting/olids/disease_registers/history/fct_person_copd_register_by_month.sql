{{
    config(
        materialized='table',
        cluster_by=['month_end_date', 'person_id'],
        tags=['monthly-full'])
}}

-- COPD register at each completed month-end, using calculate_copd_register.

WITH register AS (
    {{ calculate_copd_register(reference_dates=ltc_register_history_month_ends()) }}
)

SELECT
    register.person_id,
    register.reference_date AS month_end_date,
    spine.practice_code,
    register.earliest_diagnosis_date,
    register.latest_diagnosis_date,
    register.latest_resolved_date,
    register.earliest_unresolved_diagnosis_date,
    register.qof_rule_number,
    CASE register.qof_rule_number
        {% for rule_number, rule_label in copd_qof_rule_labels().items() %}
        WHEN {{ rule_number }} THEN '{{ rule_label }}'
        {% endfor %}
    END AS qof_rule_applied
FROM register
INNER JOIN {{ ref('int_segmentation_person_month_spine') }} AS spine
    ON register.person_id = spine.person_id
    AND register.reference_date = spine.month_end_date
WHERE register.is_on_register
    AND spine.is_active

{#-
    Combine the NICE heart failure measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
{% macro nice_heart_failure_union(reference='current') %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_ras_order_date,
    is_ras_in_period,
    latest_beta_blocker_order_date,
    is_beta_blocker_in_period,
    latest_hf_licensed_beta_blocker_order_date,
    is_hf_licensed_beta_blocker_in_period,
    latest_mra_order_date,
    is_mra_in_period,
    latest_sglt2_order_date,
    is_sglt2_in_period,
    NULL::DATE AS diagnosis_date,
    NULL::BOOLEAN AS has_reduced_ef_category,
    NULL::BOOLEAN AS has_mildly_reduced_ef_category,
    NULL::BOOLEAN AS has_preserved_ef_category,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_heart_failure_four_pillars_ind317' if reference == 'current' else 'fct_person_heart_failure_four_pillars_ind317_by_month') }}

UNION ALL

SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    NULL::DATE AS latest_ras_order_date,
    NULL::BOOLEAN AS is_ras_in_period,
    NULL::DATE AS latest_beta_blocker_order_date,
    NULL::BOOLEAN AS is_beta_blocker_in_period,
    NULL::DATE AS latest_hf_licensed_beta_blocker_order_date,
    NULL::BOOLEAN AS is_hf_licensed_beta_blocker_in_period,
    NULL::DATE AS latest_mra_order_date,
    NULL::BOOLEAN AS is_mra_in_period,
    NULL::DATE AS latest_sglt2_order_date,
    NULL::BOOLEAN AS is_sglt2_in_period,
    diagnosis_date,
    NULL::BOOLEAN AS has_reduced_ef_category,
    NULL::BOOLEAN AS has_mildly_reduced_ef_category,
    NULL::BOOLEAN AS has_preserved_ef_category,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_heart_failure_confirmation_ind192' if reference == 'current' else 'fct_person_heart_failure_confirmation_ind192_by_month') }}

UNION ALL

SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    NULL::DATE AS latest_ras_order_date,
    NULL::BOOLEAN AS is_ras_in_period,
    NULL::DATE AS latest_beta_blocker_order_date,
    NULL::BOOLEAN AS is_beta_blocker_in_period,
    NULL::DATE AS latest_hf_licensed_beta_blocker_order_date,
    NULL::BOOLEAN AS is_hf_licensed_beta_blocker_in_period,
    NULL::DATE AS latest_mra_order_date,
    NULL::BOOLEAN AS is_mra_in_period,
    NULL::DATE AS latest_sglt2_order_date,
    NULL::BOOLEAN AS is_sglt2_in_period,
    diagnosis_date,
    has_reduced_ef_category,
    has_mildly_reduced_ef_category,
    has_preserved_ef_category,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_heart_failure_ef_category_ind318' if reference == 'current' else 'fct_person_heart_failure_ef_category_ind318_by_month') }}
{% endmacro %}

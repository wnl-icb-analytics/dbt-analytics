/*
QMUL GP BP Registry — programme-specific configuration

Tuneables that only make sense in the context of the QMUL research cohort.
General pregnancy/HDP window mechanics (used by reusable upstream models)
live in pregnancy_hdp_config.sql.

Usage:
  DATEDIFF('day', span_start, span_end) >= {{ qmul_gp_bp_registry_min_span_months() }} * 30

Override at runtime:
  dbt run --vars '{"qmul_gp_bp_registry_min_span_months": 24}'
*/

{% macro qmul_gp_bp_registry_min_reading_gap_weeks() %}
    {{ var('qmul_gp_bp_registry_min_reading_gap_weeks', 4) }}
{% endmacro %}

{% macro qmul_gp_bp_registry_min_qualifying_readings() %}
    {{ var('qmul_gp_bp_registry_min_qualifying_readings', 4) }}
{% endmacro %}

{% macro qmul_gp_bp_registry_min_span_months() %}
    {{ var('qmul_gp_bp_registry_min_span_months', 36) }}
{% endmacro %}

{% macro qmul_gp_bp_registry_extraction_start_date() %}
    '{{ var('qmul_gp_bp_registry_extraction_start_date', '2010-01-01') }}'
{% endmacro %}

{% macro qmul_gp_bp_registry_extraction_end_date() %}
    '{{ var('qmul_gp_bp_registry_extraction_end_date', '2025-12-31') }}'
{% endmacro %}

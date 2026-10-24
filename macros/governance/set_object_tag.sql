{% macro set_object_tag(tag, value) %}
  {% set materialized = config.get('materialized') %}
  {% if materialized != 'ephemeral' %}
    {% set object_type = {'view': 'view', 'semantic_view': 'semantic view'}.get(materialized, 'table') %}
    alter {{ object_type }} {{ this }} set tag DATA_LAKE.CONTROL.{{ tag }} = '{{ value }}'
  {% endif %}
{% endmacro %}

{% macro set_lifecycle_tag(status='LEGACY') %}
  {{ set_object_tag('LIFECYCLE_STATUS', status) }}
{% endmacro %}

{% macro set_external_validation_tag() %}
  {#- Models whose meta.indicator has an external_validation block -#}
  {% set validation = (config.get('meta') or {}).get('indicator', {}).get('external_validation', {}) %}
  {% if validation.get('status') %}
    {{ set_object_tag('EXTERNAL_VALIDATION', 'VALIDATED' if validation.status == 'validated' else 'NOT_VALIDATED') }}
  {% endif %}
{% endmacro %}

{% macro set_managed_by_tag() %}
  {{ set_object_tag('MANAGED_BY', 'dbt-analytics') }}
{% endmacro %}

{% macro set_object_tags(tags) %}
  {#- One statement for several tags; tags is a list of [name, value] pairs -#}
  {% set materialized = config.get('materialized') %}
  {% if materialized != 'ephemeral' and tags %}
    {% set object_type = {'view': 'view', 'semantic_view': 'semantic view'}.get(materialized, 'table') %}
    {% set assignments = [] %}
    {% for tag in tags %}
      {% do assignments.append('DATA_LAKE.CONTROL.' ~ tag[0] ~ " = '" ~ (tag[1] | replace("'", "''")) ~ "'") %}
    {% endfor %}
    alter {{ object_type }} {{ this }} set tag {{ assignments | join(', ') }}
  {% endif %}
{% endmacro %}

{% macro set_object_tag(tag, value) %}
  {{ set_object_tags([[tag, value]]) }}
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

{% macro set_governance_tags() %}
  {#- MANAGED_BY on every model, plus PRIMARY_KEY where meta.primary_key is declared -#}
  {% set key = primary_key_columns() %}
  {% if key %}
    {{ set_object_tags([['MANAGED_BY', 'dbt-analytics'], ['PRIMARY_KEY', key | join(', ')]]) }}
  {% else %}
    {{ set_object_tags([['MANAGED_BY', 'dbt-analytics']]) }}
  {% endif %}
{% endmacro %}

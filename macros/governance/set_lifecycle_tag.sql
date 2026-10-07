{% macro set_lifecycle_tag(status='LEGACY') %}
  {% if config.get('materialized') != 'ephemeral' %}
    {% set object_type = 'view' if config.get('materialized') == 'view' else 'table' %}
    alter {{ object_type }} {{ this }} set tag DATA_LAKE.CONTROL.LIFECYCLE_STATUS = '{{ status }}'
  {% endif %}
{% endmacro %}

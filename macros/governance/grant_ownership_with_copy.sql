{% macro grant_ownership_with_copy(role='ANALYST') %}
  {% if config.get('materialized') == 'ephemeral' %}
    {# Ephemeral models don't create database objects, so skip grants #}
  {% elif model.fqn[1] == 'partner' %}
    {# Partner objects keep the schema owner roles; ANALYST must not own policy-protected tables or the policy mapping #}
  {% else %}
    {% set object_type = 'TABLE' %}

    {% if config.get('materialized') == 'view' %}
      {% set object_type = 'VIEW' %}
    {% elif config.get('materialized') == 'table' %}
      {% set object_type = 'TABLE' %}
    {% elif config.get('materialized') == 'incremental' %}
      {% set object_type = 'TABLE' %}
    {% elif config.get('materialized') == 'snapshot' %}
      {% set object_type = 'TABLE' %}
    {% elif config.get('materialized') == 'seed' %}
      {% set object_type = 'TABLE' %}
    {% else %}
      {% set object_type = 'TABLE' %}
    {% endif %}

    GRANT OWNERSHIP ON {{ object_type }} {{ this }} TO ROLE {{ role }} COPY CURRENT GRANTS
  {% endif %}
{% endmacro %}
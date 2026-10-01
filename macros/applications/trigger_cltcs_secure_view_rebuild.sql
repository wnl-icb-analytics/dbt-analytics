{% macro trigger_cltcs_secure_view_rebuild() %}
  {#
    Fire the app-domain secure-view rebuild iff this build materialised at
    least one model tagged 'cltcs_secure_source' (i.e. a published C_LTCS
    source a secure view reads). Runs only on real run/build invocations,
    never on parse/seed/docs/test-only runs.
  #}
  {% if execute and flags.WHICH in ['run', 'build'] %}
    {% set touched = [] %}
    {% for res in results %}
      {% if res.node.resource_type == 'model'
            and res.status == 'success'
            and 'cltcs_secure_source' in res.node.tags %}
        {% do touched.append(res.node.name) %}
      {% endif %}
    {% endfor %}
    {% if touched | length > 0 %}
      {% set app_db = 'APPLICATIONS' if target.name in ['prod', 'snowflake-prod']
                      else 'DEV__APPLICATIONS' %}
      {{ log('🔐 CLTCS secure-view rebuild triggered by: ' ~ touched | join(', '), info=True) }}
      CALL {{ app_db }}.CLTCS.REBUILD_DRIFTED_SECURE_VIEWS(FALSE)
    {% endif %}
  {% endif %}
{% endmacro %}
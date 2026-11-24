{% macro primary_key_columns() %}
  {#- config.meta.primary_key; scripts/primary_keys/primary_keys.py check requires tests to enforce it -#}
  {% set key = (config.get('meta') or {}).get('primary_key') or [] %}
  {{ return([key] if key is string else key) }}
{% endmacro %}

{% macro set_primary_key_constraint() %}
  {#- NORELY: Snowflake does not enforce the key, and RELY would let the optimiser drop joins
      that a duplicated key needs, returning wrong results before the grain test fails -#}
  {% set materialized = config.get('materialized') %}
  {#- Unquoted names are case-insensitive, so quote only names that need it (spaces,
      punctuation) or columns whose YAML sets quote: true -#}
  {% set quoted = [] %}
  {% for column in primary_key_columns() %}
    {% set ns = namespace(plain=true) %}
    {% for character in column %}
      {% if character not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
        {% set ns.plain = false %}
      {% endif %}
    {% endfor %}
    {% set quote = ((model.columns or {}).get(column) or {}).get('quote') %}
    {% do quoted.append(column if ns.plain and not quote else adapter.quote(column)) %}
  {% endfor %}
  {% if materialized == 'table' and quoted %}
    alter table {{ this }} add primary key ({{ quoted | join(', ') }}) norely
  {% elif materialized == 'incremental' %}
    {#- The table outlives the run, so replace any earlier key; drop errors when there is none -#}
    execute immediate $$
    begin
      begin
        alter table {{ this }} drop primary key;
      exception
        when statement_error then null;
      end;
      {% if quoted %}
      alter table {{ this }} add primary key ({{ quoted | join(', ') }}) norely;
      {% else %}
      alter table {{ this }} unset tag DATA_LAKE.CONTROL.PRIMARY_KEY;
      {% endif %}
    end;
    $$
  {% endif %}
{% endmacro %}

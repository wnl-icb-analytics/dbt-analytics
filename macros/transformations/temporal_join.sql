{% macro temporal_join(
        fact_table,
        fact_date_column,
        dimension_table,
        join_key='person_id',
        join_type='inner',
        valid_from_col='effective_start_date',
        valid_to_col='effective_end_date'
) %}
  {#-
      Point-in-time join: match each fact row to the dimension version whose validity
      window [valid_from, valid_to) contains the fact date.

      Compared at DATE granularity (fact date and both bounds cast to DATE), so a version
      valid at any time on day D is returned for an anchor of day D -- the state as at the
      end of that day -- even when the bounds are timestamps (e.g. dbt snapshot run times).
      Where several versions share the anchor day, the last one of that day takes precedence.
      The half-open window keeps this to <=1 version per key (no fan-out).

      join_type    'inner' (default) drops fact rows with no valid version (eligibility
                   gate); 'left' keeps every fact row (covariate enrichment, coalesce in
                   the caller).
      valid_from_col / valid_to_col
                   SCD validity columns. Default effective_start_date / effective_end_date;
                   pass dbt_valid_from / dbt_valid_to for a dbt snapshot. NULL valid_to =
                   current version.

      Emits a join fragment aliased `f` (fact) / `d` (dimension); one call per dimension in
      its own CTE.
  -#}
  {{- fact_table }} f
  {{ join_type | upper }} JOIN {{ dimension_table }} d
    ON f.{{ join_key }} = d.{{ join_key }}
    AND cast(f.{{ fact_date_column }} as date) >= cast(d.{{ valid_from_col }} as date)
    AND (d.{{ valid_to_col }} IS NULL OR cast(f.{{ fact_date_column }} as date) < cast(d.{{ valid_to_col }} as date))
{% endmacro %}
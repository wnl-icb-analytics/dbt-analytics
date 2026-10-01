{% macro scd2_valid_between(
        relation,
        start_date,
        end_date,
        valid_from_col='dbt_valid_from',
        valid_to_col='dbt_valid_to'
) %}

    {#-
        Return the SCD2 versions from `relation` whose validity window
        [valid_from, valid_to) overlaps the closed query window [start_date, end_date].

        A row is returned when it was valid at any point within the window (written
        date-on-the-left to match `temporal_join`s predicate ordering):
            end_date >= valid_from
            AND (valid_to IS NULL OR start_date < valid_to)
        A NULL `valid_to` marks the current (open-ended) version.

        Returns EVERY overlapping version. A key with several versions spanning the
        window yields multiple rows - the macro does NOT deduplicate. If you need one
        row per key, dedupe in the caller, e.g.:
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY person_id ORDER BY dbt_valid_from DESC
            ) = 1

        Args:
            relation       SCD2 table, e.g. ref('..._snapshot') or source(...).
            start_date     Window start; pass a 'YYYY-MM-DD' string (quoted here).
            end_date       Window end;   pass a 'YYYY-MM-DD' string (quoted here).
                           Use start_date == end_date for a point-in-time (as-at) read.
            valid_from_col SCD2 valid-from column (default dbt_valid_from).
            valid_to_col   SCD2 valid-to column   (default dbt_valid_to).
    -#}

    SELECT *
    FROM {{ relation }}
    WHERE '{{ end_date }}' >= {{ valid_from_col }}
      AND ({{ valid_to_col }} IS NULL OR '{{ start_date }}' < {{ valid_to_col }})

{% endmacro %}

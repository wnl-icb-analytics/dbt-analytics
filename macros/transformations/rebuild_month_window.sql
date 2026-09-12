{% macro rebuild_month_window(month_column, stored_month_column=none) %}
{#-
    Incremental month selection shared by the monthly history models. Emits a
    parenthesised predicate matching every month later than the newest month
    already stored, plus the latest completed month. Replacing the latest
    completed month on every run lets it pick up late-arriving records.

    Callers keep their own is_incremental() guard, so the predicate is only
    emitted on a run that adds to rows already in the table.

    Args:
        month_column         month-end column being filtered, qualified with a
                             table alias where the query uses one
        stored_month_column  month-end column in the model's own table, where
                             it is named differently from month_column
-#}
{%- set stored_month_column = stored_month_column or month_column -%}
(
    {{ month_column }} > (
        select coalesce(max({{ stored_month_column }}), '1900-01-01'::date)
        from {{ this }}
    )
    or {{ month_column }} = last_day(dateadd('month', -1, current_date))
)
{%- endmacro %}

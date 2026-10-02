-- 2025-10-13: dear future Tom, I removed an inner join that is necessary for tables which don't have unique_service_request_identifier
-- the inner join messed up the cyp201 deduplication process, so if you add it back in make sure it works ok
{% macro deduplicate_table(
        table,
        partition_cols,
        order_cols,
        order_direction='DESC'
    ) %}

    {#
        This macro deduplicates a table by

        dedup_table (dbt table ref):
            a ref to the raw csds table to be deduplicated

        partition_cols (list[str]):
            a list of column(s) present in the dedup_table which the deduplication occurs over

        order_cols (list[str]):
            a list of column(s) to order by when picking the row to keep

        order_direction (str, optional):
            'DESC' (default) keeps the row with the highest order_cols values, 'ASC' the
            lowest. Applies to every order column. Use 'ASC' with date_recorded when the
            table feeds point-in-time views: keeping the latest entry would hide the
            record at dates when an earlier entry already existed. 'ASC' sorts NULLs
            last explicitly, so a row with a value is kept over one without, whatever
            the session's DEFAULT_NULL_ORDERING. 'DESC' keeps the session default.

        Example usage:
    #}

    {% if partition_cols | length == 0 %}
        {{ exceptions.raise_compiler_error("You must provide at least one partition column to deduplicate_table.") }}
    {% endif %}

    {% if order_cols is not iterable or order_cols is string or order_cols | length == 0 %}
        {{ exceptions.raise_compiler_error("You must provide order_cols as a non-empty list to deduplicate_table.") }}
    {% endif %}

    {% if order_direction | upper not in ['ASC', 'DESC'] %}
        {{ exceptions.raise_compiler_error("order_direction must be 'ASC' or 'DESC' in deduplicate_table.") }}
    {% endif %}

    SELECT
        tbl.*
    FROM {{ table }} AS tbl
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
        {%- for col in partition_cols %}
            {{ "tbl." ~ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
        ORDER BY
        {%- for col in order_cols %}
            tbl.{{ col }} {{ order_direction | upper }}{% if order_direction | upper == 'ASC' %} NULLS LAST{% endif %}{% if not loop.last %},{% endif %}
        {%- endfor %}
    ) = 1

{% endmacro %}

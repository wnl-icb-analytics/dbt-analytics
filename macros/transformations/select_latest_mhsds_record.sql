{% macro select_latest_mhsds_record(
        mhsds_table,
        partition_cols = [],
        tie_breaker_cols = []
    ) %}

    {# Select the newest reported version of a record that recurs across periods.
       Source row order is descending: within one submitted file, a later row
       for the same key is treated as the later correction. int_mhsds_currency_primary_diagnosis
       cannot use this macro and deliberately reads source rows ascending, to
       follow the NHS England grouper order.
       Ranking uses the submission's reporting period, but the macro returns
       tbl.*, so callers publish the period recorded on the record itself. #}
    {% if partition_cols | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "You must provide at least one partition column to select_latest_mhsds_record."
        ) }}
    {% endif %}

    select tbl.*
    from {{ mhsds_table }} as tbl
    inner join {{ ref('stg_mhsds_activesubmission') }} as submission
        on tbl.uniq_submission_id = submission.uniq_submission_id
    qualify row_number() over (
        partition by
            {%- for col in partition_cols %}
                tbl.{{ col }}{% if not loop.last %}, {% endif %}
            {%- endfor %}
        order by
            submission.reporting_period_end_date desc
            , tbl.effective_from desc nulls last
            , tbl.uniq_submission_id desc
            , tbl.row_number desc nulls last
            {%- for col in tie_breaker_cols %}
            , tbl.{{ col }} desc
            {%- endfor %}
    ) = 1

{% endmacro %}

{% macro select_accepted_mhsds_period_records(mhsds_table, record_id) %}

    {# Filter records to the accepted file for each provider and reporting period,
       keeping one row per source record identifier. #}
    select tbl.*
    from {{ mhsds_table }} as tbl
    inner join {{ ref('stg_mhsds_activesubmission') }} as submission
        on tbl.uniq_submission_id = submission.uniq_submission_id
    {{ mhsds_one_row_per_record('tbl.' ~ record_id, 'tbl') }}

{% endmacro %}

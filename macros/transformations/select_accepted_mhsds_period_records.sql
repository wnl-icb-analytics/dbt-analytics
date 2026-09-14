{% macro select_accepted_mhsds_period_records(mhsds_table) %}

    {# Filter records to the accepted file for each provider and reporting period. #}
    select tbl.*
    from {{ mhsds_table }} as tbl
    inner join {{ ref('stg_mhsds_activesubmission') }} as submission
        on tbl.uniq_submission_id = submission.uniq_submission_id

{% endmacro %}

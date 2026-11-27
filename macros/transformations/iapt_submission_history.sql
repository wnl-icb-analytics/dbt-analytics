{% macro iapt_unseen_submission(submission_id) -%}
    {%- if is_incremental() -%}
        not exists (
            select 1
            from {{ this }} as retained
            where retained.submission_id = {{ submission_id }}
        )
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}

{% macro iapt_remove_withdrawn_submissions() -%}
    {%- if execute -%}
        delete from {{ this }} as retained
        -- An empty upstream list must fail its test, not erase retained history.
        where exists (select 1 from {{ ref('stg_iapt_activesubmission') }})
        and not exists (
            select 1
            from {{ ref('stg_iapt_activesubmission') }} as accepted
            where accepted.submission_id = retained.submission_id
        )
    {%- endif -%}
{%- endmacro %}

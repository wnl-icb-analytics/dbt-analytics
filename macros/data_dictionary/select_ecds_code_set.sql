{#
    One row per SNOMED code ever published in an ECDS ETOS code set.
    Definition columns come from the newest release that lists the code as
    live, so deprecated codes keep their last real description. Status and
    valid_to_date come from the newest release that lists the code at all.

    code_set_name: ecds_code_set_history.code_set_name, e.g. 'DISCHARGE DESTINATION'.
    extra_columns: history columns populated for this code set.
    has_valid_to_date: true when the code set has deprecated codes with end dates.
#}
{% macro select_ecds_code_set(code_set_name, extra_columns=[], has_valid_to_date=false) %}
with code_set as (
    select *
    from {{ ref('ecds_code_set_history') }}
    where code_set_name = '{{ code_set_name }}'
),

releases as (
    select
        snomed_code,
        min_by(etos_version, etos_release_date) as first_etos_version,
        max_by(etos_version, etos_release_date) as last_etos_version,
        max_by(is_deprecated, etos_release_date) as is_deprecated,
        max_by(valid_to_date, etos_release_date) as valid_to_date,
        max(etos_release_date) = (
            select max(etos_release_date) from {{ ref('ecds_code_set_history') }}
        ) as is_in_latest_release
    from code_set
    group by snomed_code
),

definitions as (
    select
        snomed_code,
        ecds_description,
        snomed_uk_preferred_term,
        -- Releases before v3.1.0 hold the fully specified name in snomed_description.
        coalesce(snomed_fully_specified_name, snomed_description) as snomed_fully_specified_name,
        ecds_group1,
        {%- for column in extra_columns %}
        {{ column }},
        {%- endfor %}
        ecds_unique_id,
        valid_from_date
    from code_set
    qualify row_number() over (
        partition by snomed_code
        order by is_deprecated, etos_release_date desc
    ) = 1
)

select
    definitions.*,
    {%- if has_valid_to_date %}
    releases.valid_to_date,
    {%- endif %}
    releases.is_in_latest_release,
    releases.is_deprecated,
    releases.first_etos_version,
    releases.last_etos_version
from definitions
inner join releases
    on definitions.snomed_code = releases.snomed_code
{% endmacro %}

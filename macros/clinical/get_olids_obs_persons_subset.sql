{% macro get_olids_obs_persons_subset(inclusion_code_list,
                            exclusion_code_list,
                            date_from,
                            date_to=none) %}

 {#
      Returns person_ids that:
        - have at least one observation with a code in inclusion_code_list
        - have no observations with a code in exclusion_code_list
        - only considers observations in [date_from, date_to]

      Emits a plain SELECT (no top-level WITH) so it composes as a CTE body or a
      FROM-derived-table.

      Parameters:
        inclusion_code_list (list[str]):
            Codes defining the inclusion criteria.
            If empty or null, no persons will be included.

        exclusion_code_list (list[str]):
            Codes defining the exclusion criteria.
            If empty or null, no persons will be excluded.

        date_from (SQL expression):
            Lower bound for clinical_effective_date (inclusive),
            e.g. DATEADD(year, -3, CURRENT_DATE).

        date_to (SQL expression, optional):
            Upper bound for clinical_effective_date (inclusive), e.g. an index date.
            Default none => no upper bound (current behaviour: "now" is the implicit cap).

      Output:
        A query returning a single column:
            - person_id
    #}

    select distinct person_id
    from {{ ref('stg_olids_observation') }}
    where
    {% if inclusion_code_list %}
        mapped_concept_code in {{ to_sql_list(inclusion_code_list) }}
        and clinical_effective_date >= {{ date_from }}
        {% if date_to is not none %}and clinical_effective_date <= {{ date_to }}{% endif %}
    {% else %}
    --No inclusions gives the empty set
        1 = 0
    {% endif %}
    and person_id not in (
        select person_id
        from {{ ref('stg_olids_observation') }}
        where
        {% if exclusion_code_list %}
            mapped_concept_code in {{ to_sql_list(exclusion_code_list) }}
            and clinical_effective_date >= {{ date_from }}
            {% if date_to is not none %}and clinical_effective_date <= {{ date_to }}{% endif %}
        {% else %}
        --No exclusions gives the empty set (nobody is excluded)
            1 = 0
        {% endif %}
    )

{% endmacro %}

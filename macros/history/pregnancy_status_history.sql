{% macro pregnancy_status_history(start_date, end_date) %}

{#-
    Report whether each person had an active pregnancy at any point in the closed window
    [start_date, end_date], reconstructed from coded pregnancy / delivery observations.

    Adapted from fct_person_pregnancy_status (on 17/07/2026), which evaluates "currently pregnant" as at
    CURRENT_DATE using the heuristic: a person is pregnant for ~9 months from a pregnancy
    code until a later delivery/outcome code supersedes it. Here that logic is anchored on
    the query window instead of "now":
      - only observations up to end_date are considered (no peeking past the window);
      - a pregnancy code counts if it falls within 9 months before start_date (so its
        ~9-month episode can still overlap the window) and is not superseded by a later
        delivery code.

    Pass start_date == end_date for a point-in-time ("pregnant as at date") read.

    Emits a plain SELECT (no top-level WITH) so it composes as a CTE body or FROM-subquery.

    Notes:
      - The source model additionally restricts to non-male individuals; that demographic
        filter is intentionally omitted here (apply it in the caller if needed).
      - A pregnancy that both started before and delivered inside the window is not flagged
        (mirrors fct_person_pregnancy_status which this code is based on)

    Args:
        start_date  window start, 'YYYY-MM-DD' (quoted here).
        end_date    window end,   'YYYY-MM-DD' (quoted here).

    Returns: person_id, was_pregnant (boolean).
-#}

    select
        person_id,
        coalesce(
            latest_preg_date is not null
            and latest_preg_date >= dateadd(month, -9, '{{ start_date }}'::date)
            and (latest_delivery_date is null or latest_preg_date > latest_delivery_date),
            false
        ) as was_pregnant
    from (
        select
            person_id,
            max(case when is_pregnancy_code
                      and clinical_effective_date <= '{{ end_date }}'::date
                     then clinical_effective_date end) as latest_preg_date,
            max(case when is_delivery_outcome_code
                      and clinical_effective_date <= '{{ end_date }}'::date
                     then clinical_effective_date end) as latest_delivery_date
        from {{ ref('int_pregnancy_observations_all') }}
        group by person_id
    )

{% endmacro %}

{% macro mhsds_is_crisis_referral(team_type_alias, priority_column) -%}
    {#- NHSE mental health currency crisis-referral rule: the referral's team
        type is a crisis team, or it is a team type whose crisis status depends
        on an urgent clinical response priority (1, 2 or 4). Callers supply the
        priority column because MHSDS names it differently by layer. -#}
    coalesce(
        coalesce({{ team_type_alias }}.is_crisis_referral, false)
        or (
            coalesce({{ team_type_alias }}.crisis_requires_urgent_priority, false)
            and {{ priority_column }} in ('1', '2', '4')
        )
        , false
    )
{%- endmacro %}

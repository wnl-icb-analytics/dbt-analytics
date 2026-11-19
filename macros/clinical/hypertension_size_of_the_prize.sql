{% macro hypertension_size_of_the_prize(people, years=3, cost='nhs_social_care', prefix='') %}
{#-
    Heart attacks, strokes and deaths prevented, and costs avoided, when more people with
    hypertension are treated to target (UCLPartners Size of the Prize method).
    Args: people is a SQL expression for the extra people treated to target;
          years is the period the benefit is counted over (UCLPartners uses 3; 1 gives per year);
          cost is 'nhs_social_care' or 'uclp'; prefix is prepended to the output column names.
    Returns: comma-separated columns heart_attacks_prevented, strokes_prevented,
             deaths_prevented and costs_avoided_gbp, with rates from the
             hypertension_size_of_the_prize seed.
-#}
{%- set cost_columns = {'nhs_social_care': 'cost_per_event_gbp', 'uclp': 'uclp_cost_per_event_gbp'} -%}
{%- if cost not in cost_columns -%}
    {{ exceptions.raise_compiler_error("hypertension_size_of_the_prize cost must be 'nhs_social_care' or 'uclp'") }}
{%- endif -%}
{%- set rates = ref('hypertension_size_of_the_prize') -%}
{%- for outcome, column in [('heart_attack', 'heart_attacks_prevented'), ('stroke', 'strokes_prevented'), ('death', 'deaths_prevented')] %}
    ({{ people }}) * {{ years }} / (
        select treatment_years * people_treated_per_event_prevented from {{ rates }} where outcome = '{{ outcome }}'
    ) as {{ prefix }}{{ column }},
{%- endfor %}
    ({{ people }}) * {{ years }} * (
        select sum(coalesce({{ cost_columns[cost] }}, 0) / (treatment_years * people_treated_per_event_prevented))
        from {{ rates }}
    ) as {{ prefix }}costs_avoided_gbp
{%- endmacro %}

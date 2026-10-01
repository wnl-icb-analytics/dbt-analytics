{% macro asthma_management_history(as_of) %}

{#-
    Reconstruct the int_asthma_management feature flags as-at a point in time.

    int_asthma_management derives its flags over CURRENT_DATE()-anchored windows (a 3-year
    look-back for the diagnosis/testing/ACT/salbutamol-only flags, 12 months for the
    medication-recency flags). This macro reproduces the same logic anchored on `as_of`
    instead of "now", so asthma-management state can be read for a past date. The
    live int_asthma_management model calls this with as_of = current_date(); the monthly
    covariate panel calls it per index date.

    Emits a plain SELECT (no top-level WITH) so it composes as a CTE body or FROM-derived-table.

    Occurrence-date caveat: reconstruction filters by observation/order occurrence date, so it
    reflects when events happened, not what was coded/known on `as_of` (late-arriving or
    retrospectively-coded records appear earlier than they truly were).

    Args:
        as_of  a SQL date expression for the anchor point, e.g. current_date() or
               cast('2026-07-20' as date). Do NOT pass a bare 'YYYY-MM-DD' string.

    Returns: person_id + the 7 boolean flags (same columns as int_asthma_management).
-#}

{%- set asthma_code_list = dbt_utils.get_column_values(
        ref('raw_phenolab_aic_definitions'), 'code',
        where="definition_name = 'asthma_SNOMED'") -%}
{%- set tests_code_list = dbt_utils.get_column_values(
        ref('asthma_features_codelist'), 'code',
        where="definition_name IN ('spirometry_SNOMED', 'peak_expiratory_flow_rate_SNOMED')") -%}
{%- set act_code_list = dbt_utils.get_column_values(
        ref('asthma_features_codelist'), 'code',
        where="definition_name = 'asthma_control_test_SNOMED'") -%}
{%- set salbutamol_code_list = dbt_utils.get_column_values(
        ref('asthma_features_codelist'), 'code',
        where="definition_name = 'salbutamol_inhaler_medication_SNOMED'") -%}
{%- set preventor_code_list = dbt_utils.get_column_values(
        ref('asthma_features_codelist'), 'code',
        where="definition_name = 'non_salbutamol_inhaler_medication_SNOMED'") -%}

{%- set date_from = "dateadd(year, -3, " ~ as_of ~ ")" -%}
{%- set date_to = as_of -%}

    select
        p.id as person_id,
        dnt.person_id is not null as diagnosis_no_testing,
        tnd.person_id is not null as testing_no_diagnosis,
        act.person_id is not null as diagnosis_no_act,
        so.person_id  is not null as salbutamol_only,
        sr.person_id  is not null as salbutamol_repeats,
        rabm.person_id is not null as recent_antibacterial_medications,
        rpn.person_id  is not null as recent_prednisolone_medications
    from {{ ref('stg_olids_person') }} p

    -- asthma diagnosis, no spirometry / PEFR testing
    left join (
        {{ get_olids_obs_persons_subset(
            inclusion_code_list=asthma_code_list, exclusion_code_list=tests_code_list,
            date_from=date_from, date_to=date_to) }}
    ) dnt on p.id = dnt.person_id

    -- testing, no asthma diagnosis
    left join (
        {{ get_olids_obs_persons_subset(
            inclusion_code_list=tests_code_list, exclusion_code_list=asthma_code_list,
            date_from=date_from, date_to=date_to) }}
    ) tnd on p.id = tnd.person_id

    -- asthma diagnosis, no asthma control test
    left join (
        {{ get_olids_obs_persons_subset(
            inclusion_code_list=asthma_code_list, exclusion_code_list=act_code_list,
            date_from=date_from, date_to=date_to) }}
    ) act on p.id = act.person_id

    -- salbutamol, no preventer medication
    left join (
        {{ get_olids_obs_persons_subset(
            inclusion_code_list=salbutamol_code_list, exclusion_code_list=preventor_code_list,
            date_from=date_from, date_to=date_to) }}
    ) so on p.id = so.person_id

    -- 3+ salbutamol orders in the 12 months to as_of
    left join (
        select person_id
        from {{ ref('int_asthma_medications_all') }}
        where mapped_concept_code in {{ to_sql_list(salbutamol_code_list) }}
            and order_date between dateadd(month, -12, {{ as_of }}) and {{ as_of }}
        group by person_id
        having count(*) >= 3
    ) sr on p.id = sr.person_id

    -- antibacterial order in the 365 days to as_of (matches int_antibacterial_medications_all
    -- is_recent_12m = datediff(day, order_date, CURRENT_DATE) <= 365, re-anchored on as_of)
    left join (
        select distinct person_id
        from {{ ref('int_antibacterial_medications_all') }}
        where order_date between dateadd(day, -365, {{ as_of }}) and {{ as_of }}
    ) rabm on p.id = rabm.person_id

    -- prednisolone order in the 12 months to as_of
    left join (
        select distinct mo.person_id
        from ({{ get_medication_orders(bnf_code='0603020T0') }}) mo
        where mo.order_date between dateadd(month, -12, {{ as_of }}) and {{ as_of }}
    ) rpn on p.id = rpn.person_id

{% endmacro %}

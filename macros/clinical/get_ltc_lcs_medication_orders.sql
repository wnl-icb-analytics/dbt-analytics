{% macro get_ltc_lcs_medication_orders(valuesets, match_paths='both', include_date_recorded=false) %}

{#
    Returns medication orders for one or more LTC LCS valuesets via two parallel paths:
      - 'mapped': medication_order.mapped_concept_code = expanded_concepts.snomed_code
                  (uses terminology-server-expanded SNOMED codes)
      - 'source': medication_order.medication_order_source_concept_id -> enriched_concept_map.source_concept_id
                  -> original_codes.original_code (uses raw EMIS codes preserved pre-translation)
    Path 'source' recovers orders for valuesets where the terminology server failed to
    translate EMIS codes to SNOMED. When both paths match the same order, the 'mapped'
    row wins via the qualify ordering.

    Note: original_codes.include_children is not yet expanded (parent codes only).

    Params:
      valuesets    - comma-separated list of valueset_id or valueset_friendly_name tokens
      match_paths  - 'both' (default), 'mapped', or 'source'
      include_date_recorded - also return EMIS save time for same-day ordering.
#}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_orders requires valuesets parameter") }}
{%- endif -%}

{%- set allowed_paths = ['both', 'mapped', 'source'] -%}
{%- if match_paths not in allowed_paths -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_orders match_paths must be one of: both, mapped, source") }}
{%- endif -%}

{# Normalise valuesets into a clean token list, then render quoted forms #}
{%- set valueset_tokens = [] -%}
{%- for raw in valuesets.replace("'", "").split(",") -%}
    {%- set token = raw | trim -%}
    {%- if token -%}{%- do valueset_tokens.append(token) -%}{%- endif -%}
{%- endfor -%}
{%- if valueset_tokens | length == 0 -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_orders requires non-empty valuesets parameter") }}
{%- endif -%}
{%- set valuesets_quoted = "'" ~ valueset_tokens | join("','") ~ "'" -%}
{%- set valuesets_upper_quoted = "'" ~ (valueset_tokens | map('upper') | join("','")) ~ "'" -%}

with matched_orders as (
{%- if match_paths in ['both', 'mapped'] %}
    select
        mo.id as medication_order_id,
        mo.medication_statement_id,
        mo.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        mo.clinical_effective_date as order_date,
        {%- if include_date_recorded %}
        mo.date_recorded,
        {%- endif %}
        mo.medication_name as order_medication_name,
        mo.dose as order_dose,
        mo.quantity_value as order_quantity_value,
        mo.quantity_unit as order_quantity_unit,
        mo.duration_days as order_duration_days,
        mo.estimated_cost,
        mo.statement_medication_name,
        mo.mapped_concept_code as concept_code,
        mo.mapped_concept_display as concept_display,
        ec.valueset_id,
        vs.valueset_friendly_name,
        bnf.bnf_code,
        bnf.bnf_name,
        'mapped' as match_path
    from {{ ref('stg_olids_medication_order') }} as mo
    inner join {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
        on mo.mapped_concept_code = ec.snomed_code
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on ec.valueset_id = vs.valueset_id
    join {{ ref('int_patient_person_unique') }} as pp
        on mo.patient_id = pp.patient_id
    left join {{ ref('stg_reference_bnf_latest') }} as bnf
        on mo.mapped_concept_code = bnf.snomed_code
    where mo.clinical_effective_date is not null
        and (
            ec.valueset_id in ({{ valuesets_quoted }})
            or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
        )
{%- endif %}
{%- if match_paths == 'both' %}
    union all
{%- endif %}
{%- if match_paths in ['both', 'source'] %}
    select
        mo.id as medication_order_id,
        mo.medication_statement_id,
        mo.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        mo.clinical_effective_date as order_date,
        {%- if include_date_recorded %}
        mo.date_recorded,
        {%- endif %}
        mo.medication_name as order_medication_name,
        mo.dose as order_dose,
        mo.quantity_value as order_quantity_value,
        mo.quantity_unit as order_quantity_unit,
        mo.duration_days as order_duration_days,
        mo.estimated_cost,
        mo.statement_medication_name,
        mo.mapped_concept_code as concept_code,
        mo.mapped_concept_display as concept_display,
        oc.valueset_id,
        vs.valueset_friendly_name,
        bnf.bnf_code,
        bnf.bnf_name,
        'source' as match_path
    from {{ ref('stg_olids_medication_order') }} as mo
    inner join {{ ref('stg_olids_enriched_concept_map') }} as ecm
        on mo.medication_order_source_concept_id = ecm.source_concept_id
    inner join {{ ref('stg_reference_ltc_lcs_original_codes') }} as oc
        on ecm.source_code = oc.original_code
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on oc.valueset_id = vs.valueset_id
    join {{ ref('int_patient_person_unique') }} as pp
        on mo.patient_id = pp.patient_id
    left join {{ ref('stg_reference_bnf_latest') }} as bnf
        on mo.mapped_concept_code = bnf.snomed_code
    where mo.clinical_effective_date is not null
        and (
            oc.valueset_id in ({{ valuesets_quoted }})
            or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
        )
{%- endif %}
)
select *
from matched_orders
qualify row_number() over (
    partition by medication_order_id, valueset_id
    order by case when match_path = 'mapped' then 0 else 1 end
) = 1

{% endmacro %}

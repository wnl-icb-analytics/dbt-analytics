{% macro get_ltc_lcs_medication_statements_latest(valuesets, match_paths='both') %}

{#
    Returns the latest medication statement per (person, valueset) for one or more LTC LCS
    valuesets via two parallel paths:
      - 'mapped': medication_statement.mapped_concept_code = expanded_concepts.snomed_code
      - 'source': medication_statement.medication_statement_source_concept_id -> enriched_concept_map.source_concept_id
                  -> original_codes.original_code
    Latest uses clinical date, EMIS save time, then record ID descending.
    Matching the same record through both paths prefers the 'mapped' row.

    P2 permits mapped concepts and requested descendants only through public or
    equivalent local roots. Other local roots retain exact source matching.
    The legacy source path also includes permitted mapped matches under P2.

    Params:
      valuesets    - comma-separated list of valueset_id or valueset_friendly_name tokens
      match_paths  - 'both' (default), 'mapped', or 'source'
#}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_statements_latest requires valuesets parameter") }}
{%- endif -%}

{%- set allowed_paths = ['both', 'mapped', 'source'] -%}
{%- if match_paths not in allowed_paths -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_statements_latest match_paths must be one of: both, mapped, source") }}
{%- endif -%}

{# Normalise valuesets into a clean token list, then render quoted forms #}
{%- set valueset_tokens = [] -%}
{%- for raw in valuesets.replace("'", "").split(",") -%}
    {%- set token = raw | trim -%}
    {%- if token -%}{%- do valueset_tokens.append(token) -%}{%- endif -%}
{%- endfor -%}
{%- if valueset_tokens | length == 0 -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_medication_statements_latest requires non-empty valuesets parameter") }}
{%- endif -%}
{%- set valuesets_quoted = "'" ~ valueset_tokens | join("','") ~ "'" -%}
{%- set valuesets_upper_quoted = "'" ~ (valueset_tokens | map('upper') | join("','")) ~ "'" -%}

with permitted_mapped_codes as (
    {{ ltc_lcs_permitted_mapped_codes(valuesets_quoted, valuesets_upper_quoted) }}
),
matched_statements as (
    select
        ms.id as medication_statement_id,
        ms.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        ms.clinical_effective_date as statement_date,
        ms.medication_name as statement_medication_name,
        ms.dose as statement_dose,
        ms.quantity_value as statement_quantity_value,
        ms.quantity_unit as statement_quantity_unit,
        ms.date_recorded,
        ms.authorisation_type_code,
        ms.authorisation_type_display,
        ms.issue_method,
        ms.is_active,
        ms.cancellation_date,
        ms.expiry_date,
        ms.age_at_event,
        ms.mapped_concept_code as concept_code,
        ms.mapped_concept_display as concept_display,
        ec.valueset_id,
        vs.valueset_friendly_name,
        bnf.bnf_code,
        bnf.bnf_name,
        'mapped' as match_path
    from {{ ref('stg_olids_medication_statement') }} as ms
    inner join permitted_mapped_codes as ec
        on ms.mapped_concept_code = ec.snomed_code
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on ec.valueset_id = vs.valueset_id
    join {{ ref('int_patient_person_unique') }} as pp
        on ms.patient_id = pp.patient_id
    left join {{ ref('stg_reference_bnf_latest') }} as bnf
        on ms.mapped_concept_code = bnf.snomed_code
    where ms.clinical_effective_date is not null
        and (
            ec.valueset_id in ({{ valuesets_quoted }})
            or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
        )
{%- if match_paths in ['both', 'source'] %}
    union all
{%- endif %}
{%- if match_paths in ['both', 'source'] %}
    select
        ms.id as medication_statement_id,
        ms.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        ms.clinical_effective_date as statement_date,
        ms.medication_name as statement_medication_name,
        ms.dose as statement_dose,
        ms.quantity_value as statement_quantity_value,
        ms.quantity_unit as statement_quantity_unit,
        ms.date_recorded,
        ms.authorisation_type_code,
        ms.authorisation_type_display,
        ms.issue_method,
        ms.is_active,
        ms.cancellation_date,
        ms.expiry_date,
        ms.age_at_event,
        ms.mapped_concept_code as concept_code,
        ms.mapped_concept_display as concept_display,
        oc.valueset_id,
        vs.valueset_friendly_name,
        bnf.bnf_code,
        bnf.bnf_name,
        'source' as match_path
    from {{ ref('stg_olids_medication_statement') }} as ms
    inner join {{ ref('stg_olids_enriched_concept_map') }} as ecm
        on ms.medication_statement_source_concept_id = ecm.source_concept_id
    inner join {{ ref('stg_reference_ltc_lcs_original_codes') }} as oc
        on ecm.source_code = oc.original_code
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on oc.valueset_id = vs.valueset_id
    join {{ ref('int_patient_person_unique') }} as pp
        on ms.patient_id = pp.patient_id
    left join {{ ref('stg_reference_bnf_latest') }} as bnf
        on ms.mapped_concept_code = bnf.snomed_code
    where ms.clinical_effective_date is not null
        and (
            oc.valueset_id in ({{ valuesets_quoted }})
            or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
        )
{%- endif %}
)
select *
from matched_statements
qualify row_number() over (
    partition by person_id, valueset_id
    order by
        statement_date desc nulls last,
        date_recorded desc nulls last,
        medication_statement_id desc,
        case when match_path = 'mapped' then 0 else 1 end
) = 1

{% endmacro %}

{% macro get_ltc_lcs_observations(valuesets, match_paths='both', include_date_recorded=false) %}

{#
    Returns observations for one or more LTC LCS valuesets via two parallel paths:
      - 'mapped': observation.mapped_concept_code = expanded_concepts.snomed_code
                  (uses terminology-server-expanded SNOMED codes)
      - 'source': observation.observation_source_concept_id -> enriched_concept_map.source_concept_id
                  -> original_codes.original_code (uses raw EMIS codes preserved pre-translation)
    Path 'source' recovers observations for valuesets where the terminology server failed
    to translate EMIS codes to SNOMED. When both paths match the same observation, the
    'mapped' row wins via the qualify ordering.

    Public and equivalent EMIS-local roots permit mapped concepts and XML-requested
    descendants. Other local roots match their original source code only.

    Params:
      valuesets    - comma-separated list of valueset_id or valueset_friendly_name tokens
      match_paths  - 'both' (default), 'mapped', or 'source'
      include_date_recorded - also return date_recorded, the EMIS save time, for same-day ordering.
#}

{%- if valuesets is none -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations requires valuesets parameter") }}
{%- endif -%}

{%- set allowed_paths = ['both', 'mapped', 'source'] -%}
{%- if match_paths not in allowed_paths -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations match_paths must be one of: both, mapped, source") }}
{%- endif -%}

{# Normalise valuesets into a clean token list, then render quoted forms #}
{%- set valueset_tokens = [] -%}
{%- for raw in valuesets.replace("'", "").split(",") -%}
    {%- set token = raw | trim -%}
    {%- if token -%}{%- do valueset_tokens.append(token) -%}{%- endif -%}
{%- endfor -%}
{%- if valueset_tokens | length == 0 -%}
    {{ exceptions.raise_compiler_error("get_ltc_lcs_observations requires non-empty valuesets parameter") }}
{%- endif -%}
{%- set valuesets_quoted = "'" ~ valueset_tokens | join("','") ~ "'" -%}
{%- set valuesets_upper_quoted = "'" ~ (valueset_tokens | map('upper') | join("','")) ~ "'" -%}

with permitted_mapped_codes as (
    {{ ltc_lcs_permitted_mapped_codes(valuesets_quoted, valuesets_upper_quoted) }}
),
matched_observations as (
{%- if match_paths in ['both', 'mapped'] %}
    select
        o.id as observation_id,
        o.patient_id,
        o.person_id,
        o.clinical_effective_date,
        {%- if include_date_recorded %}
        o.date_recorded,
        {%- endif %}
        o.result_value,
        o.result_units_source_concept_id,
        o.result_unit_display,
        o.result_text,
        o.allergy_medication_name,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.mapped_concept_id as concept_id,
        o.mapped_concept_code as concept_code,
        o.mapped_concept_display as concept_display,
        o.episodicity_source_concept_id,
        ec.valueset_id,
        vs.valueset_friendly_name,
        'mapped' as match_path
    from {{ ref('stg_olids_observation') }} as o
    inner join permitted_mapped_codes as ec
        on o.mapped_concept_code = ec.snomed_code
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on ec.valueset_id = vs.valueset_id
    where (
        ec.valueset_id in ({{ valuesets_quoted }})
        or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
    )
{%- endif %}
{%- if match_paths == 'both' %}
    union all
{%- endif %}
{%- if match_paths in ['both', 'source'] %}
    select
        o.id as observation_id,
        o.patient_id,
        o.person_id,
        o.clinical_effective_date,
        {%- if include_date_recorded %}
        o.date_recorded,
        {%- endif %}
        o.result_value,
        o.result_units_source_concept_id,
        o.result_unit_display,
        o.result_text,
        o.allergy_medication_name,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.mapped_concept_id as concept_id,
        o.mapped_concept_code as concept_code,
        o.mapped_concept_display as concept_display,
        o.episodicity_source_concept_id,
        oc.valueset_id,
        vs.valueset_friendly_name,
        'source' as match_path
    from {{ ref('stg_olids_observation') }} as o
    inner join {{ ref('stg_olids_enriched_concept_map') }} as ecm
        on o.observation_source_concept_id = ecm.source_concept_id
    inner join {{ ref('stg_reference_ltc_lcs_original_codes') }} as oc
        on ecm.source_code = oc.original_code
    left join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on oc.valueset_id = vs.valueset_id
    where (
        oc.valueset_id in ({{ valuesets_quoted }})
        or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
    )
{%- endif %}
)
select *
from matched_observations
qualify row_number() over (
    partition by observation_id, valueset_id
    order by case when match_path = 'mapped' then 0 else 1 end
) = 1

{% endmacro %}

{% macro ltc_lcs_permitted_mapped_codes(valuesets_quoted, valuesets_upper_quoted) %}

{# Intersect root permissions with the supplied expansion to retain its exceptions.
   Public-only sets retain specialised expansion rules without restriction. #}
with selected_roots as (
    select
        oc.valueset_id,
        oc.original_code,
        oc.translated_to_snomed_code,
        oc.include_children,
        oc.is_refset,
        oc.translation_equivalence
    from {{ ref('stg_reference_ltc_lcs_original_codes') }} as oc
    inner join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
        on oc.valueset_id = vs.valueset_id
    where oc.valueset_id in ({{ valuesets_quoted }})
        or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }})
),
permitted_roots as (
    select valueset_id, translated_to_snomed_code, include_children
    from selected_roots
    where not regexp_like(original_code, '.*1000006[0-9]{3}$')
        or lower(translation_equivalence) = 'equivalent'
),
permitted_codes as (
    select valueset_id, translated_to_snomed_code as snomed_code
    from permitted_roots
    union
    select root.valueset_id, hierarchy.sub_type_id::varchar as snomed_code
    from permitted_roots as root
    inner join {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} as hierarchy
        on hierarchy.super_type_id::varchar = root.translated_to_snomed_code
    where root.include_children
    union
    -- Refset roots can have no translated concept. Resolve their members directly.
    select root.valueset_id, member.snomed_code
    from selected_roots as root
    inner join ({{ ltc_lcs_refset_members() }}) as member
        on member.refset_id = root.original_code
    where root.is_refset
        and (not regexp_like(root.original_code, '.*1000006[0-9]{3}$')
            or lower(root.translation_equivalence) = 'equivalent')
)
select distinct ec.valueset_id, ec.snomed_code
from {{ ref('stg_reference_ltc_lcs_expanded_concepts') }} as ec
inner join {{ ref('stg_reference_ltc_lcs_valuesets') }} as vs
    on ec.valueset_id = vs.valueset_id
where (ec.valueset_id in ({{ valuesets_quoted }})
    or upper(vs.valueset_friendly_name) in ({{ valuesets_upper_quoted }}))
    and (
        -- Public-only sets retain their existing expansion, including refsets.
        not exists (
            select 1 from selected_roots as root
            where root.valueset_id = ec.valueset_id
                and regexp_like(root.original_code, '.*1000006[0-9]{3}$')
        )
        or exists (
            select 1 from permitted_codes as permitted
            where permitted.valueset_id = ec.valueset_id
                and permitted.snomed_code = ec.snomed_code
        )
    )

{% endmacro %}

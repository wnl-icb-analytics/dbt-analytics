{{ config(tags=['daily']) }}

with codes as (
    select code as snomed_code, min(code_description) as code_description
    from {{ ref('stg_reference_combined_codesets') }}
    where upper(cluster_id) in ({{ observation_evidence_cluster_ids() }})
    group by code
), context as (
    select r.source_id::varchar as snomed_code,
        max(iff(r.type_id = 408730004, r.destination_id::varchar, null)) as procedure_context,
        count(distinct iff(r.type_id = 408730004, r.destination_id, null)) as procedure_context_count,
        boolor_agg(r.type_id = 408732007 and r.destination_id <> 410604004) as other_subject,
        boolor_agg(r.type_id = 408731000 and r.destination_id not in (410511007, 410512000, 410513005)) as unsupported_time_context
    from {{ ref('stg_nhsd_snomed_sct_relationship') }} as r
    inner join codes on r.source_id::varchar = codes.snomed_code
    where r.active and r.characteristic_type_id = 900000000000011006
        and r.type_id in (408730004, 408731000, 408732007)
    group by r.source_id
), terms as (
    select codes.snomed_code,
        coalesce(c.fully_specified_name, codes.code_description) as concept_name,
        lower(coalesce(c.fully_specified_name, codes.code_description)) as wording,
        regexp_substr(c.fully_specified_name, '\\(([^()]*)\\)$', 1, 1, 'e', 1) as semantic_tag,
        context.procedure_context, context.procedure_context_count,
        coalesce(context.other_subject, false) as other_subject,
        coalesce(context.unsupported_time_context, false) as unsupported_time_context
    from codes
    left join {{ ref('snomed_concept') }} as c using (snomed_code)
    left join context using (snomed_code)
), classified as (
    select *, wording like '%offered%' as is_offer,
        coalesce(procedure_context = '397943006', false)
            or regexp_like(wording, '.*(planned|awaited).*') as is_planned_or_awaited,
        case
            when other_subject or unsupported_time_context or procedure_context_count > 1 then null
            when procedure_context not in ('443390004', '385644000', '397943006', '385658003', '385652002') then null
            when regexp_like(wording, '.*(not done|not performed|cancelled|canceled|not indicated|contraindicat).*') then null
            when procedure_context = '443390004' or regexp_like(wording, '.*(declined|refused).*') then 'declined'
            when regexp_like(wording, '.*(invitation|invited|labeling amended|labelling amended|synchronization|synchronisation).*') then 'administrative'
            when regexp_like(wording, '.*(offered|requested|awaited|planned|referral|referred|refer to).*')
                or procedure_context in ('385644000', '397943006') then 'requested_or_referred'
            when regexp_like(wording, '.*(advi[cs]|education|educated|counsel|recommendation|discuss|leaflet|written information|signpost|health promotion).*') then 'advised'
            when procedure_context in ('385658003', '385652002') then 'completed'
            when regexp_like(wording, '.*(seen by|seen in|completed| done|commenced| started).*') then 'completed'
            when semantic_tag in ('procedure', 'regime/therapy') then 'completed'
            when semantic_tag in ('finding', 'observable entity', 'disorder') then 'finding'
        end as rule_evidence_type
    from terms
)
select classified.snomed_code, concept_name, semantic_tag,
    coalesce(reviewed.evidence_type, rule_evidence_type) as evidence_type, is_offer, is_planned_or_awaited,
    case
        when reviewed.evidence_type is not null then reviewed.basis
        when rule_evidence_type is null then 'Requires review: missing, conflicting or unsupported context'
        when procedure_context = '443390004' then 'SNOMED procedure context: declined'
        when rule_evidence_type in ('administrative', 'requested_or_referred', 'advised') then 'SNOMED context and activity wording'
        when rule_evidence_type = 'finding' then 'SNOMED semantic tag: finding or observable'
        when procedure_context in ('385658003', '385652002') then 'SNOMED procedure context: done or started'
        when semantic_tag in ('procedure', 'regime/therapy') then 'SNOMED default procedure context: completed'
        else 'Explicit attendance or completion wording'
    end as classification_basis
from classified
left join {{ ref('snomed_evidence_type_override') }} as reviewed
    on classified.snomed_code = reviewed.snomed_code

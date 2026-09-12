with versions as (
    select
        {{ dbt_utils.generate_surrogate_key(['referral_id', 'care_contact_id', 'care_activity_id']) }}
            as source_record_id
        , {{ dbt_utils.generate_surrogate_key(['referral_id', 'care_contact_id']) }} as contact_source_record_id
        , submission_id
        , source_row_id
        , provider_organisation_code
        , person_id
        , referral_id
        , care_contact_id
        , care_activity_id
        , pathway_id
        , nullif(trim(code_proc_and_proc_status), '') as procedure_expression
        , nullif(upper(trim(find_scheme_in_use)), '') as finding_scheme_code
        , nullif(trim(code_find), '') as finding_code
        , nullif(upper(trim(validated_finding_code)), '') as nhsd_validated_finding_code
        , nullif(trim(code_obs), '') as observation_code
        , nullif(trim(obs_value), '') as observation_value
        , nullif(trim(unit_measure), '') as unit_of_measurement_code
        , clin_contact_dur_of_care_act as clinical_activity_duration_minutes
        , reporting_period_start_date
        , reporting_period_end_date
        , unique_month_id
        , dataset_version
        , file_type
        , source_file_received_at
        , source_loaded_at
        , count(distinct reporting_period_end_date) over (
            partition by referral_id, care_contact_id, care_activity_id
        ) as reported_period_count
        , min(person_id) over (partition by referral_id, care_contact_id, care_activity_id)
            is distinct from max(person_id) over (partition by referral_id, care_contact_id, care_activity_id)
            as has_person_identifier_changed
    from {{ ref('stg_iapt_care_activity_history') }}
    qualify row_number() over (
        partition by referral_id, care_contact_id, care_activity_id
        order by unique_month_id desc nulls last, source_file_received_at desc nulls last,
            try_to_number(submission_id) desc nulls last, try_to_number(source_row_id) desc nulls last,
            submission_id desc, source_row_id desc
    ) = 1
)

, parsed as (
    select
        v.*
        , {{ iapt_procedure_expression_type('v.procedure_expression') }} as procedure_expression_type
        , {{ iapt_procedure_focus_code('v.procedure_expression') }} as procedure_concept_code
        , {{ iapt_procedure_context_code('v.procedure_expression') }} as procedure_context_code
        , {{ iapt_asserted_procedure_code('v.procedure_expression') }} as asserted_procedure_code
        , case v.finding_scheme_code when '01' then 'ICD-10' when '04' then 'SNOMED CT' end
            as finding_coding_system
        , iff(finding_coding_system = 'ICD-10', {{ iapt_icd10_lookup_code('v.finding_code') }}, null)
            as finding_code_normalised
        -- NHSD pads three-character codes with X or 9 and marks unmatched codes -3.
        , iff(
            finding_coding_system = 'ICD-10' and coalesce(v.nhsd_validated_finding_code, '') <> '-3'
            , {{ iapt_icd10_lookup_code('coalesce(v.nhsd_validated_finding_code, v.finding_code)') }}, null
        ) as finding_icd10_lookup_code
        , iff(finding_coding_system = 'SNOMED CT', v.finding_code, null) as finding_snomed_code
        , try_to_decimal(v.observation_value, 38, 9) as observation_value_numeric
    from versions as v
)

, activities as (
    select
        p.source_record_id
        , 'IAPT' as source_dataset
        , 'IDS202' as source_table
        , p.care_activity_id
        , p.care_contact_id
        , p.referral_id
        , p.pathway_id
        , p.contact_source_record_id
        , p.person_id
        , b.sk_patient_id
        , p.has_person_identifier_changed

        , t.clinical_date
        , t.clinical_time
        , t.clinical_at
        , t.clinical_time_precision
        , t.clinical_time_basis
        , t.clinical_date_status
        , t.source_derived_date as source_derived_activity_date
        , t.is_source_date_inconsistent
        , t.is_submitted_contact_linked
        , t.is_submitted_contact_referral_consistent
        , t.is_submitted_contact_person_consistent
        , p.clinical_activity_duration_minutes

        , p.procedure_expression is not null as has_procedure
        , p.procedure_expression
        , p.procedure_expression_type
        , iff(p.procedure_expression is null, null, p.procedure_expression_type <> 'bare_concept')
            as has_procedure_qualifier
        , p.procedure_concept_code
        , procedure_concept.preferred_term as procedure_name
        , regexp_substr(procedure_concept.fully_specified_name, '[(]([^()]*)[)]$', 1, 1, 'e', 1)
            as procedure_semantic_tag
        , p.procedure_context_code
        , procedure_context.preferred_term as procedure_context_description
        -- The common label keeps the context so an offered or planned therapy never reads as delivered.
        , case p.procedure_expression_type
            when 'bare_concept' then procedure_concept.preferred_term
            when 'procedure_context' then coalesce(procedure_concept.preferred_term, p.procedure_concept_code)
                || ' (context: ' || coalesce(procedure_context.preferred_term, p.procedure_context_code) || ')'
            when 'unrecognised_refinement' then coalesce(procedure_concept.preferred_term, p.procedure_concept_code)
                || ' (unrecognised refinement)'
        end as procedure_description
        , p.asserted_procedure_code
        , case
            when p.procedure_expression is null then 'code_missing'
            when p.procedure_expression_type = 'unparsed' then 'unparsed_expression'
            when procedure_concept.snomed_code is null then 'code_unmatched'
            when p.procedure_expression_type = 'unrecognised_refinement' then 'refinement_unrecognised'
            else 'labelled'
        end as procedure_label_status
        , therapy.therapy_type_category
        , therapy.legacy_therapy_type_code

        , p.finding_code is not null as has_finding
        , p.finding_scheme_code
        , finding_scheme.description as finding_scheme_description
        , p.finding_coding_system
        , p.finding_code
        , p.nhsd_validated_finding_code
        , p.finding_snomed_code
        , {{ iapt_icd10_precision('p.finding_code_normalised') }} as finding_icd10_precision
        , iff(
            p.finding_coding_system = 'ICD-10'
            , coalesce(length(p.finding_code_normalised) = 3 and length(p.finding_icd10_lookup_code) = 4
                and startswith(p.finding_icd10_lookup_code, p.finding_code_normalised), false)
            , null
        ) as is_finding_code_extended_by_nhsd
        , left(p.finding_icd10_lookup_code, 3) as finding_icd10_category_code
        , finding_icd10_category.description as finding_icd10_category_description
        -- Three-character submissions keep the category label, not NHSD's padded "unspecified" code.
        , case p.finding_coding_system
            when 'ICD-10' then iff(
                is_finding_code_extended_by_nhsd
                    or finding_icd10_precision in ('three_character', 'three_character_filler')
                , coalesce(finding_icd10_category.description, finding_icd10.description)
                , finding_icd10.description
            )
            when 'SNOMED CT' then finding_snomed.preferred_term
        end as finding_description
        , case
            when p.finding_code is null then 'code_missing'
            when p.finding_scheme_code is null then 'coding_scheme_missing'
            when p.finding_coding_system is null then 'coding_scheme_unrecognised'
            when p.finding_coding_system = 'ICD-10' and p.nhsd_validated_finding_code = '-3'
                then 'invalid_code_supplied'
            when finding_description is not null then 'labelled'
            else 'code_unmatched'
        end as finding_label_status

        , p.observation_code is not null or p.observation_value is not null as has_observation
        , p.observation_code
        , observation_concept.preferred_term as observation_description
        , case
            when p.observation_code is null then 'code_missing'
            when observation_concept.snomed_code is null then 'code_unmatched'
            else 'labelled'
        end as observation_label_status
        -- Guidance p41: scored assessments belong in IDS606/607, not the observation field.
        , observation_scale.concept_code is not null as is_assessment_observable_in_observation
        , p.observation_value
        , p.observation_value_numeric
        , case
            when p.observation_value is null then 'value_missing'
            when p.observation_value_numeric is not null then 'numeric'
            else 'not_numeric'
        end as observation_value_parse_status
        , p.unit_of_measurement_code
        , unit.description as unit_of_measurement_description
        , unit.unit_symbol as unit_of_measurement_symbol
        , case
            when p.unit_of_measurement_code is null then 'code_missing'
            when unit.code is null then 'code_unmatched'
            else 'labelled'
        end as unit_of_measurement_label_status

        , p.provider_organisation_code
        , provider.organisation_name as provider_organisation_name
        , p.reported_period_count
        , p.submission_id
        , p.source_row_id
        , p.reporting_period_start_date
        , p.reporting_period_end_date
        , p.unique_month_id
        , p.dataset_version
        , p.file_type
        , p.source_file_received_at
        , p.source_loaded_at
    from parsed as p
    inner join {{ ref('int_iapt_care_activity_timing') }} as t
        on p.submission_id = t.submission_id
        and p.care_activity_id = t.care_activity_id
    left join {{ ref('stg_iapt_bridging') }} as b
        on p.person_id = b.person_id
    left join {{ ref('organisation') }} as provider
        on p.provider_organisation_code = provider.organisation_code
    left join {{ ref('snomed_concept') }} as procedure_concept
        on p.procedure_concept_code = procedure_concept.snomed_code
    left join {{ ref('snomed_concept') }} as procedure_context
        on p.procedure_context_code = procedure_context.snomed_code
    left join {{ ref('iapt_therapy_type_definitions') }} as therapy
        on p.asserted_procedure_code = therapy.snomed_code
    left join {{ ref('mhsds_care_activity_code_lookup') }} as finding_scheme
        on finding_scheme.code_set_name = 'finding_scheme'
        and p.finding_scheme_code = finding_scheme.code
    left join {{ ref('icd10_code') }} as finding_icd10
        on p.finding_icd10_lookup_code = finding_icd10.code
    left join {{ ref('icd10_code') }} as finding_icd10_category
        on left(p.finding_icd10_lookup_code, 3) = finding_icd10_category.code
    left join {{ ref('snomed_concept') }} as finding_snomed
        on p.finding_snomed_code = finding_snomed.snomed_code
    left join {{ ref('snomed_concept') }} as observation_concept
        on p.observation_code = observation_concept.snomed_code
    left join {{ ref('iapt_assessment_scale') }} as observation_scale
        on p.observation_code = observation_scale.concept_code
        and observation_scale.is_latest_definition
    left join {{ ref('clinical_unit_of_measurement') }} as unit
        on p.unit_of_measurement_code = unit.code
)

-- Every reference is qualified: the parent fact is joined only here.
, parent_choice as (
    select
        a.*
        -- A parent is published only when the same-submission contact agrees and the published
        -- contact row names the same referral, contact and non-null person.
        , iff(
            a.is_submitted_contact_linked and a.is_submitted_contact_referral_consistent
                and a.is_submitted_contact_person_consistent
                and a.person_id = c.person_id
                and a.referral_id = c.referral_id
                and a.care_contact_id = c.care_contact_id
            , a.contact_source_record_id, null
        ) as parent_record_id
    from activities as a
    left join {{ ref('fct_iapt_care_contact') }} as c
        on a.contact_source_record_id = c.source_record_id
)

-- One source in scope: type and model follow parent_record_id.
select
    *
    , iff(parent_record_id is not null, 'care_contact', null) as parent_record_type
    , iff(parent_record_id is not null, 'fct_iapt_care_contact', null) as parent_model_name
from parent_choice

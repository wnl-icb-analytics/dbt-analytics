with labelled as (
    select
        r.*
        , b.sk_patient_id
        , iff(r.coding_scheme_kind = 'fixed_snomed', 'SNOMED CT', scheme.description)
            as coding_scheme_name
        , case
            when r.coding_scheme_kind = 'fixed_snomed'
                or (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '06')
                or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '04')
                or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '03') then 'SNOMED CT'
            when (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '04')
                or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '02')
                or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '01') then 'Read v2'
            when (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '05')
                or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '03')
                or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '02') then 'CTV3'
            when r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '01' then 'ICD-10'
            else scheme.description
        end as clinical_code_system
        , coalesce(snomed.preferred_term, icd.description, read_code.term) as clinical_description
        , case when snomed.snomed_code is not null then 'Dictionary.SNOMED.Concept'
            when icd.code is not null then 'Dictionary.dbo.Diagnosis'
            when read_code.code is not null then read_code.definition_source end as clinical_label_source
        , read_code.match_type as read_code_match_type
        , read_code.snomed_ct_code::varchar as dictionary_snomed_code
        , mapped_snomed.preferred_term as dictionary_snomed_description
        , case
            when r.clinical_code is null then 'code_missing'
            when r.coding_scheme_kind <> 'fixed_snomed' and scheme.code is null
                then 'coding_scheme_unrecognised'
            when snomed.snomed_code is not null or icd.code is not null or read_code.code is not null then 'labelled'
            when r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) not in ('04', '05', '06')
                then 'reference_not_available'
            else 'code_or_expression_unmatched'
        end as clinical_label_status
        , coalesce(unit.description, unit_alias.description) as unit_of_measurement_description
        , coalesce(unit.unit_symbol, unit_alias.unit_symbol) as unit_of_measurement_symbol
        , coalesce(unit.match_type, iff(unit_alias.snomed_code is not null, 'csds_etos_alias', null))
            as unit_of_measurement_match_type
        , coalesce(unit.definition_source, unit_alias.definition_source) as unit_of_measurement_definition_source
        , scale.assessment_tool_name
        , scale.specification_version as assessment_definition_version
        , response.response_description as clinical_value_description
        , response.is_non_score_response as is_assessment_response_non_score
        , try_to_decimal(r.clinical_value, 38, 9) as clinical_value_numeric
        , case
            when r.clinical_value is null then 'value_missing'
            when clinical_value_numeric is null then 'not_numeric_or_out_of_range'
            when not regexp_like(trim(r.clinical_value), '[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)')
                then 'numeric_format_unverified'
            when not regexp_like(trim(r.clinical_value), '[+-]?([0-9]+([.][0-9]{0,9}0*)?|[.][0-9]{1,9}0*)')
                then 'numeric_rounded'
            else 'numeric'
        end as clinical_value_parse_status
        , case
            when r.clinical_record_type not in ('referral_assessment', 'activity_assessment') then null
            when r.clinical_value is null then 'value_missing'
            when response.is_non_score_response then 'known_non_score'
            when response.response_code is not null then 'enumerated_response'
            when scale.numeric_range_count = 1 and clinical_value_parse_status = 'numeric'
                and clinical_value_numeric between scale.minimum_numeric_value and scale.maximum_numeric_value
                and scale.decimal_places is not null
                and clinical_value_numeric = round(clinical_value_numeric, scale.decimal_places)
                then 'within_published_range'
            when scale.concept_code is null then 'reference_not_available'
            else 'response_unmatched'
        end as assessment_response_status
        , reference.concept_code is not null
            and r.source_table = 'CYP202' as is_assessment_code_in_activity_component
    from {{ ref('int_csds_clinical_record') }} as r
    left join {{ ref('stg_csds_bridging') }} as b on r.person_id = b.person_id
    left join {{ ref('csds_activity_code_lookup') }} as scheme
        on scheme.code_set_name = r.coding_scheme_kind || '_scheme'
        and trim(r.coding_scheme_code) = scheme.code
    left join {{ ref('stg_dictionary_snomed_concept') }} as snomed
        on trim(r.clinical_code) = snomed.snomed_code
        and (r.coding_scheme_kind = 'fixed_snomed'
            or (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '06')
            or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '04')
            or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '03'))
    left join {{ ref('stg_dictionary_dbo_diagnosis') }} as icd
        on {{ clean_icd10_code('upper(trim(r.clinical_code))') }} = upper(icd.code)
        and r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '01'
    left join {{ ref('read_code') }} as read_code
        on trim(r.clinical_code) = read_code.code
        and (
            (read_code.coding_system = 'read_v2' and (
                (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '04')
                or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '02')
                or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '01')))
            or (read_code.coding_system = 'ctv3' and (
                (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '05')
                or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '03')
                or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '02')))
        )
    left join {{ ref('stg_dictionary_snomed_concept') }} as mapped_snomed
        on read_code.snomed_ct_code::varchar = mapped_snomed.snomed_code
    left join {{ ref('clinical_unit_of_measurement') }} as unit
        on trim(r.unit_of_measurement_code) = unit.code
    left join {{ ref('csds_observation_unit_alias') }} as unit_alias
        on unit.code is null
        and r.source_table = 'CYP202' and r.clinical_record_type = 'observation'
        and r.coding_scheme_kind = 'observation'
        and upper(trim(r.unit_of_measurement_code)) = unit_alias.source_unit_upper
        and case when trim(r.coding_scheme_code) = '03' then trim(r.clinical_code)
            else read_code.snomed_ct_code end = unit_alias.snomed_code
    left join {{ ref('csds_assessment_scale') }} as scale
        on r.clinical_record_type in ('referral_assessment', 'activity_assessment')
        and trim(r.clinical_code) = scale.concept_code
    left join {{ ref('csds_assessment_response') }} as response
        on r.clinical_record_type in ('referral_assessment', 'activity_assessment')
        and trim(r.clinical_code) = response.concept_code
        and (trim(r.clinical_value) = response.response_code
            or (try_to_decimal(r.clinical_value, 38, 9) = response.numeric_response_value
                and regexp_like(trim(r.clinical_value), '[+-]?([0-9]+([.][0-9]{0,9}0*)?|[.][0-9]{1,9}0*)')))
    left join {{ ref('csds_assessment_scale') }} as reference
        on trim(r.clinical_code) = reference.concept_code
        and (r.coding_scheme_kind = 'fixed_snomed'
            or (r.coding_scheme_kind = 'procedure' and trim(r.coding_scheme_code) = '06')
            or (r.coding_scheme_kind = 'finding' and trim(r.coding_scheme_code) = '04')
            or (r.coding_scheme_kind = 'observation' and trim(r.coding_scheme_code) = '03'))
)

select
    sk_patient_id
    , {{ dbt_utils.generate_surrogate_key([
        'source_table', 'originating_source_record_id', 'clinical_record_type'
    ]) }} as clinical_record_id
    , clinical_record_id as source_record_id
    , 'CSDS' as source_dataset
    , clinical_record_type
    , clinical_code
    , clinical_description
    , clinical_label_source
    , read_code_match_type
    , dictionary_snomed_code
    , dictionary_snomed_description
    , clinical_code_system
    , coding_scheme_code
    , coding_scheme_name
    , clinical_label_status
    , clinical_at
    , clinical_at::date as clinical_date
    , iff(clinical_time_precision = 'timestamp', clinical_at::time, null) as clinical_time
    , clinical_time_precision
    , clinical_time_basis
    , assessment_tool_name
    , clinical_value
    , clinical_value_description
    , clinical_value_numeric
    , clinical_value_parse_status
    , iff(assessment_response_status in ('enumerated_response', 'within_published_range')
        and clinical_value_parse_status = 'numeric',
        clinical_value_numeric, null) as assessment_score_numeric
    , assessment_response_status
    , is_assessment_response_non_score
    , assessment_definition_version
    , unit_of_measurement_code
    , unit_of_measurement_description
    , unit_of_measurement_symbol
    , unit_of_measurement_match_type
    , unit_of_measurement_definition_source
    , provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , immunisation_responsible_organisation_code
    , responsible.organisation_name as immunisation_responsible_organisation_name
    , person_id
    , local_patient_id
    , referral_source_record_id as referral_id
    , care_activity_source_record_id
    , unique_care_activity_identifier as activity_id
    , unique_care_contact_identifier as contact_id
    , is_care_activity_linked
    , is_care_activity_person_consistent
    , is_submitted_contact_person_consistent
    , source_clinical_date
    , source_derived_date
    , source_derived_date <> clinical_at::date as is_source_derived_date_inconsistent
    , clinical_at::date > reporting_period_end_date as is_clinical_date_after_reporting_period
    , iff(clinical_record_type = 'referral_assessment',
        clinical_at::date < reporting_period_start_date, null) as is_assessment_before_reporting_period
    , is_assessment_code_in_activity_component
    , reporting_period_start_date
    , reporting_period_end_date
    , first_reported_period_end_date
    , last_reported_period_end_date
    , accepted_source_record_count
    , source_table
    , originating_source_record_id
    , source_row_id
    , unique_submission_id as submission_id
    , source_file_received_at
    , csds_version
    , file_type
from labelled
left join {{ ref('organisation') }} as provider
    on upper(trim(labelled.provider_organisation_code)) = provider.organisation_code
left join {{ ref('organisation') }} as responsible
    on upper(trim(labelled.immunisation_responsible_organisation_code)) = responsible.organisation_code

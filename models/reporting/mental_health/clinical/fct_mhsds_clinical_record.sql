with resolved as (
    select
        r.*,
        case when r.coding_scheme_kind = 'diagnosis' then scheme.description
            else r.source_coding_scheme_description end as coding_scheme_description,
        case
            when upper(coding_scheme_description) like 'SNOMED CT%' then 'SNOMED CT'
            when upper(coding_scheme_description) in ('READ V2', 'READ CODED CLINICAL TERMS VERSION 2') then 'Read v2'
            when upper(coding_scheme_description) in ('CTV3', 'READ CODED CLINICAL TERMS VERSION 3 (CTV3)') then 'CTV3'
            else coding_scheme_description
        end as clinical_code_system,
        scheme.code is not null as is_diagnosis_scheme_recognised
    from {{ ref('int_mhsds_clinical_record') }} as r
    left join {{ ref('mhsds_diagnosis_scheme') }} as scheme
        on r.coding_scheme_kind = 'diagnosis'
        and upper(trim(r.coding_scheme_code)) = scheme.code
)
, labelled as (
    select
        r.* exclude (
            source_record_id, source_coding_scheme_description, source_clinical_description,
            source_clinical_label_status, source_standardised_snomed_description
        )
        , r.source_record_id as originating_source_record_id
        , coalesce(
            r.source_standardised_snomed_description, mapped.preferred_term
        ) as standardised_snomed_description
        , case
            when r.clinical_code_system in ('Read v2', 'CTV3') then read_label.term
            when r.source_table in ('MHS202', 'MHS609') then r.source_clinical_description
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code = '02'
                then icd.description
            when r.coding_scheme_kind = 'fixed_snomed' or r.coding_scheme_code = '06'
                then snomed.preferred_term
        end as clinical_description
        , coalesce(snomed.snomed_code, supplied_snomed.snomed_code, read_mapping.snomed_code) as snomed_code
        , case
            when snomed.snomed_code is not null then snomed.preferred_term
            when supplied_snomed.snomed_code is not null then supplied_snomed.preferred_term
            else read_mapping.snomed_code_name
        end as snomed_code_name
        , case
            when snomed.snomed_code is not null then 'Recorded as SNOMED CT'
            when supplied_snomed.snomed_code is not null then 'MHSDS supplied SNOMED'
            else read_mapping.snomed_mapping_method
        end as snomed_mapping_method
        , iff(snomed.snomed_code is null and supplied_snomed.snomed_code is null,
            read_mapping.is_assured, null) as is_snomed_mapping_assured
        , case
            when r.clinical_code is null then 'code_missing'
            when r.clinical_code_system in ('Read v2', 'CTV3')
                then iff(read_label.term is not null, 'labelled', 'reference unavailable')
            when r.source_table in ('MHS202', 'MHS609') then case
                when r.coding_scheme_kind = 'finding' and r.coding_scheme_code = '05'
                    then 'reference unavailable'
                when r.source_clinical_label_status = 'code_unmatched'
                    then 'code_or_expression_unmatched'
                when r.source_clinical_label_status = 'labelled_snomed_scheme_missing'
                    then 'labelled'
                else r.source_clinical_label_status
            end
            when r.coding_scheme_kind = 'diagnosis' and not r.is_diagnosis_scheme_recognised
                then 'coding_scheme_unrecognised'
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code = '07'
                then 'reference unavailable'
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code not in ('02', '06')
                then 'reference unavailable'
            when clinical_description is not null then 'labelled'
            else 'code_or_expression_unmatched'
        end as clinical_label_status
        , b.sk_patient_id
        , provider.organisation_name as provider_organisation_name
        , unit.unit_symbol as unit_of_measurement_symbol
        , unit.match_type as unit_of_measurement_match_type
        , unit.definition_source as unit_of_measurement_definition_source
        , scale.assessment_tool_name
        , scale.specification_version as assessment_definition_version
        , response.response_description as clinical_value_description
        , response.specification_version as assessment_response_definition_version
        , response.is_non_score_response as is_assessment_response_non_score
        , case
            when r.source_table not in ('MHS606', 'MHS607', 'MHS802') then null
            when r.clinical_value is null then 'no value'
            when response.is_non_score_response then 'non-score response'
            when response.response_code is not null then 'listed response'
            when scale.numeric_range_count = 1
                and try_to_decimal(r.clinical_value, 38, 9)
                    between scale.minimum_numeric_value and scale.maximum_numeric_value
                and try_to_double(r.clinical_value)
                    = round(try_to_decimal(r.clinical_value, 38, 9), scale.decimal_places)::double
                then 'within published range'
            when scale.concept_code is null then 'reference unavailable'
            else 'unmatched response'
        end as assessment_response_status
    from resolved as r
    left join {{ ref('snomed_concept') }} as snomed
        on trim(r.clinical_code) = snomed.snomed_code
        and r.clinical_code_system = 'SNOMED CT'
    left join {{ ref('icd10_code') }} as icd
        on replace({{ clean_icd10_code('upper(trim(r.clinical_code))') }}, '.', '') = icd.code
        and r.coding_scheme_kind = 'diagnosis'
        and r.coding_scheme_code = '02'
    left join {{ ref('snomed_concept') }} as mapped
        on trim(r.standardised_snomed_code) = mapped.snomed_code
        and r.source_table <> 'MHS202'
    left join {{ ref('snomed_concept') }} as supplied_snomed
        on r.standardised_snomed_code::varchar = supplied_snomed.snomed_code
        and (r.clinical_code_system in ('ICD-10', 'OPCS-4')) is distinct from true
    left join {{ ref('read_code') }} as read_label
        on trim(r.clinical_code) = read_label.code
        and read_label.coding_system = case r.clinical_code_system
            when 'Read v2' then 'read_v2' when 'CTV3' then 'ctv3' end
    left join {{ ref('read_code_snomed_map') }} as read_mapping
        on trim(r.clinical_code) = read_mapping.code
        and read_mapping.coding_system = case r.clinical_code_system
            when 'Read v2' then 'read_v2' when 'CTV3' then 'ctv3' end
    left join {{ ref('stg_mhsds_bridging') }} as b
        on r.person_id = b.person_id
    left join {{ ref('int_mhsds_organisation') }} as provider
        on upper(r.provider_organisation_code) = upper(provider.organisation_code)
    left join {{ ref('clinical_unit_of_measurement') }} as unit
        on trim(r.unit_of_measurement_code) = unit.code
    left join {{ ref('mhsds_assessment_scale') }} as scale
        on r.source_table in ('MHS606', 'MHS607', 'MHS802')
        and trim(r.clinical_code) = scale.concept_code
    left join {{ ref('mhsds_assessment_response') }} as response
        on r.source_table in ('MHS606', 'MHS607', 'MHS802')
        and trim(r.clinical_code) = response.concept_code
        and (trim(r.clinical_value) = response.response_code
            or try_to_double(r.clinical_value) = response.numeric_response_value::double)
)

select
    sk_patient_id
    , {{ dbt_utils.generate_surrogate_key([
        'source_table', 'originating_source_record_id', 'clinical_record_type'
    ]) }} as clinical_record_id
    , clinical_record_id as source_record_id
    , clinical_record_type

    , clinical_code
    , clinical_description
    , clinical_code_system
    , snomed_code
    , snomed_code_name
    , snomed_mapping_method
    , is_snomed_mapping_assured
    , standardised_snomed_code
    , standardised_snomed_description
    , coding_scheme_code
    , coding_scheme_description
    , coding_scheme_kind
    , clinical_label_status

    , clinical_at
    , clinical_at::date as clinical_date
    , iff(clinical_time_precision = 'date', null, clinical_at::time) as clinical_time
    , clinical_time_basis
    , clinical_time_precision

    , assessment_tool_name
    , clinical_value
    , clinical_value_description
    , try_to_decimal(clinical_value, 38, 9) as clinical_value_numeric
    , case
        when clinical_value is null then 'no value'
        when clinical_value_numeric is null then 'not numeric or out of range'
        when try_to_double(clinical_value) <> clinical_value_numeric::double
            then 'numeric (rounded)'
        else 'numeric'
    end as clinical_value_parse_status
    , iff(assessment_response_status in ('listed response', 'within published range'),
        clinical_value_numeric, null) as assessment_score_numeric
    , is_assessment_response_non_score
    , assessment_response_status
    , assessment_definition_version
    , assessment_response_definition_version

    , unit_of_measurement_code
    , unit_of_measurement_description
    , unit_of_measurement_symbol
    , unit_of_measurement_label_status
    , unit_of_measurement_match_type
    , unit_of_measurement_definition_source

    , provider_organisation_code
    , provider_organisation_name
    , source_assessment_id
    , is_assessment_parent_linked
    , is_assessment_parent_person_consistent
    , person_id
    , local_patient_id
    , referral_source_record_id
    , uniq_care_cont_id
    , care_activity_source_record_id
    , uniq_care_act_id
    , uniq_care_prof_local_id
    , care_prof_local_id
    , is_care_activity_linked
    , is_care_activity_person_consistent
    , is_care_activity_referral_consistent
    , is_care_activity_contact_consistent
    , is_care_contact_person_consistent
    , has_person_identifier_changed

    , source_timestamp
    , source_derived_date
    , is_source_date_inconsistent
    , coalesce(source_timestamp::date < '1901-01-01'::date, false)
        or coalesce(source_derived_date < '1901-01-01'::date, false) as has_source_date_sentinel
    , coalesce(clinical_at::date > reporting_period_end_date, false)
        as is_clinical_date_after_reporting_period
    , iff(
        source_table = 'MHS606'
        , coalesce(clinical_at::date < reporting_period_start_date, false)
        , null
    ) as is_assessment_before_reporting_period

    , reporting_period_start_date
    , reporting_period_end_date
    , first_reported_period_end_date
    , last_reported_period_end_date
    , reported_period_count
    , accepted_source_record_count
    , 'MHSDS' as source_dataset
    , source_table
    , originating_source_record_id
    , source_row_id
    , uniq_submission_id
    , mhsds_version
    , source_file_received_at
    , source_loaded_at
from labelled

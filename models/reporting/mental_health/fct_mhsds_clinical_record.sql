with labelled as (
    select
        r.* exclude (
            source_record_id, source_coding_scheme_description, source_clinical_description,
            source_clinical_label_status, source_standardised_snomed_description
        )
        , r.source_record_id as originating_source_record_id
        , case
            when r.coding_scheme_kind = 'diagnosis' then scheme.description
            else r.source_coding_scheme_description
        end as coding_scheme_description
        , coalesce(
            r.source_standardised_snomed_description, mapped.preferred_term
        ) as standardised_snomed_description
        , case
            when r.source_table = 'MHS202' then coalesce(
                r.source_clinical_description
                , iff(r.source_clinical_label_status = 'mapped_to_snomed',
                    r.source_standardised_snomed_description, null)
            )
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code = '02'
                then icd.description
            when r.coding_scheme_kind = 'fixed_snomed' or r.coding_scheme_code = '06'
                then snomed.preferred_term
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code in ('03', '04', '05')
                then mapped.preferred_term
        end as clinical_description
        , case
            when r.source_table = 'MHS202' then case
                when r.source_clinical_label_status = 'code_unmatched'
                    then 'code_or_expression_unmatched'
                when r.source_clinical_label_status = 'labelled_snomed_scheme_missing'
                    then 'labelled'
                else r.source_clinical_label_status
            end
            when r.clinical_code is null then 'code_missing'
            when r.coding_scheme_kind = 'diagnosis' and scheme.code is null
                then 'coding_scheme_unrecognised'
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code = '07'
                then 'reference_not_available'
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code in ('03', '04', '05')
                then iff(mapped.snomed_code is null, 'reference_not_available', 'mapped_to_snomed')
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code not in ('02', '06')
                then 'reference_not_available'
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
            when r.source_table not in ('MHS606', 'MHS607') then null
            when r.clinical_value is null then 'value_missing'
            when response.is_non_score_response then 'known_non_score'
            when response.response_code is not null then 'enumerated_response'
            when scale.numeric_range_count = 1
                and try_to_decimal(r.clinical_value, 38, 9)
                    between scale.minimum_numeric_value and scale.maximum_numeric_value
                and try_to_double(r.clinical_value)
                    = round(try_to_decimal(r.clinical_value, 38, 9), scale.decimal_places)::double
                then 'within_published_range'
            when scale.concept_code is null then 'reference_not_available'
            else 'response_unmatched'
        end as assessment_response_status
    from {{ ref('int_mhsds_clinical_record') }} as r
    left join {{ ref('mhsds_diagnosis_scheme') }} as scheme
        on r.coding_scheme_kind = 'diagnosis'
        and upper(trim(r.coding_scheme_code)) = scheme.code
    left join {{ ref('stg_dictionary_snomed_concept') }} as snomed
        on trim(r.clinical_code) = snomed.snomed_code
        and r.source_table <> 'MHS202'
        and (r.coding_scheme_kind = 'fixed_snomed' or r.coding_scheme_code = '06')
    left join {{ ref('stg_dictionary_dbo_diagnosis') }} as icd
        on {{ clean_icd10_code('upper(trim(r.clinical_code))') }} = upper(icd.code)
        and r.coding_scheme_kind = 'diagnosis'
        and r.coding_scheme_code = '02'
    left join {{ ref('stg_dictionary_snomed_concept') }} as mapped
        on trim(r.standardised_snomed_code) = mapped.snomed_code
        and r.source_table <> 'MHS202'
    left join {{ ref('stg_mhsds_bridging') }} as b
        on r.person_id = b.person_id
    left join {{ ref('int_mhsds_organisation') }} as provider
        on upper(r.provider_organisation_code) = upper(provider.organisation_code)
    left join {{ ref('clinical_unit_of_measurement') }} as unit
        on trim(r.unit_of_measurement_code) = unit.code
    left join {{ ref('mhsds_assessment_scale') }} as scale
        on r.source_table in ('MHS606', 'MHS607')
        and trim(r.clinical_code) = scale.concept_code
    left join {{ ref('mhsds_assessment_response') }} as response
        on r.source_table in ('MHS606', 'MHS607')
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
        when clinical_value is null then 'value_missing'
        when clinical_value_numeric is null then 'not_numeric_or_out_of_range'
        when try_to_double(clinical_value) <> clinical_value_numeric::double
            then 'numeric_rounded'
        else 'numeric'
    end as clinical_value_parse_status
    , iff(assessment_response_status in ('enumerated_response', 'within_published_range'),
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

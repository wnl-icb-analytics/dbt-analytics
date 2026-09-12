with referral_assessments as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "'IDS606'", 'referral_id', 'coded_ass_tool_type', 'ass_tool_comp_date', 'ass_tool_comp_time'
        ]) }} as source_record_id
        , 'IDS606' as source_table
        , 'referral' as assessment_source
        , submission_id
        , source_row_id
        , provider_organisation_code
        , person_id
        , referral_id
        , null::varchar as care_contact_id
        , null::varchar as care_activity_id
        , null::varchar as care_activity_source_record_id
        , pathway_id
        , coded_ass_tool_type as assessment_tool_code
        , pers_score as score_text
        , null::boolean as is_care_activity_linked
        , null::boolean as is_care_activity_referral_consistent
        , null::boolean as is_care_activity_contact_consistent
        , null::boolean as is_care_activity_person_consistent
        , case
            when ass_tool_comp_date is null then 'missing'
            -- Project source-epoch rule: earlier dates are placeholders.
            when ass_tool_comp_date < '1901-01-01'::date then 'source_sentinel'
            else 'recorded'
        end as clinical_date_status
        , iff(clinical_date_status = 'recorded', ass_tool_comp_date, null) as clinical_date
        , iff(clinical_date_status = 'recorded', ass_tool_comp_time, null) as clinical_time
        , 'assessment_completion' as recorded_time_basis
        , null::date as source_derived_date
        , reporting_period_start_date
        , reporting_period_end_date
        , unique_month_id
        , dataset_version
        , file_type
        , source_file_received_at
        , source_loaded_at
    from {{ ref('stg_iapt_referral_assessment_history') }}
)

-- Timing columns are renamed here so the next step cannot resolve its own aliases to them.
, activity_assessment_links as (
    select
        a.*
        , t.care_activity_id is not null as is_care_activity_linked
        , iff(t.care_activity_id is null, null, a.referral_id is not distinct from t.referral_id)
            as is_care_activity_referral_consistent
        , iff(t.care_activity_id is null, null, a.care_contact_id is not distinct from t.care_contact_id)
            as is_care_activity_contact_consistent
        , iff(t.care_activity_id is null, null, a.person_id is not distinct from t.person_id)
            as is_care_activity_person_consistent
        , t.clinical_date_status as activity_date_status
        , t.clinical_date as activity_date
        , t.clinical_time as activity_time
    from {{ ref('stg_iapt_activity_assessment_history') }} as a
    left join {{ ref('int_iapt_care_activity_timing') }} as t
        on a.submission_id = t.submission_id
        and a.care_activity_id = t.care_activity_id
)

, activity_assessments as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "'IDS607'", 'referral_id', 'care_contact_id', 'care_activity_id', 'coded_ass_tool_type'
        ]) }} as source_record_id
        , 'IDS607' as source_table
        , 'care_activity' as assessment_source
        , submission_id
        , source_row_id
        , provider_organisation_code
        , person_id
        , referral_id
        , care_contact_id
        , care_activity_id
        , {{ dbt_utils.generate_surrogate_key(['referral_id', 'care_contact_id', 'care_activity_id']) }}
            as care_activity_source_record_id
        , pathway_id
        , coded_ass_tool_type as assessment_tool_code
        , pers_score as score_text
        , is_care_activity_linked
        , is_care_activity_referral_consistent
        , is_care_activity_contact_consistent
        , is_care_activity_person_consistent
        -- IDS607 has no date; NHS England takes the contact date through the activity (DARS row 37).
        , case
            when not is_care_activity_linked then 'parent_not_linked'
            when not (is_care_activity_referral_consistent and is_care_activity_contact_consistent
                and is_care_activity_person_consistent) then 'parent_inconsistent'
            else activity_date_status
        end as clinical_date_status
        , iff(clinical_date_status = 'recorded', activity_date, null) as clinical_date
        , iff(clinical_date_status = 'recorded', activity_time, null) as clinical_time
        , 'same_submission_care_contact' as recorded_time_basis
        , dmic_completion_date as source_derived_date
        , reporting_period_start_date
        , reporting_period_end_date
        , unique_month_id
        , dataset_version
        , file_type
        , source_file_received_at
        , source_loaded_at
    from activity_assessment_links
)

, versions as (
    select
        u.*
        -- N/A was the published value from v2.1.1 until corrected to NA in v2.1.7
        -- (ETOS v2.1.22 Change Control rows 166 and 321).
        , regexp_replace(upper(u.score_text), '^N/A$', 'NA') as score_token
        , try_to_decimal(u.score_text, 38, 9) as score_numeric_parsed
        , count(distinct u.reporting_period_end_date) over (partition by u.source_record_id)
            as reported_period_count
        , min(u.person_id) over (partition by u.source_record_id)
            is distinct from max(u.person_id) over (partition by u.source_record_id)
            as has_person_identifier_changed
    from (
        select * from referral_assessments
        union all
        select * from activity_assessments
    ) as u
    qualify row_number() over (
        partition by u.source_record_id
        order by u.unique_month_id desc nulls last, u.source_file_received_at desc nulls last,
            try_to_number(u.submission_id) desc nulls last, try_to_number(u.source_row_id) desc nulls last,
            u.submission_id desc, u.source_row_id desc
    ) = 1
)

-- The most recent earlier published range for each concept. TOS revisions changed definitions
-- inside data set version 2.0 (the Diabetes Distress Scale moved from a 17-102 total to a 1-6
-- mean in 2.0.27; ETOS v2.1.22 Change Control rows 117-119), so neither the data set version nor
-- the value proves which revision a provider used.
, earlier_ranges as (
    select
        concept_code
        , specification_version
        , minimum_numeric_value
        , maximum_numeric_value
    from {{ ref('iapt_assessment_scale') }}
    where not is_latest_definition
        and numeric_range_count = 1
    qualify row_number() over (partition by concept_code order by specification_version desc) = 1
)

, scored as (
    select
        v.source_record_id
        , 'IAPT' as source_dataset
        , v.source_table
        , v.assessment_source
        , v.person_id
        , b.sk_patient_id
        , v.has_person_identifier_changed

        , v.assessment_tool_code
        , scale.assessment_tool_name
        , coalesce(scale.assessment_description, snomed.preferred_term) as observable_description
        , case
            when v.assessment_tool_code is null then 'code_missing'
            when scale.concept_code is not null then 'labelled'
            when snomed.snomed_code is not null then 'labelled_outside_specification'
            else 'code_unmatched'
        end as observable_label_status
        , scale.specification_version as assessment_definition_version
        , scale.published_values as assessment_published_values
        , scale.decimal_places as expected_decimal_places
        , v.score_text
        , v.score_numeric_parsed
        , case
            when v.score_text is null then 'value_missing'
            when v.score_numeric_parsed is not null then 'numeric'
            else 'not_numeric'
        end as score_parse_status
        , response.response_description
        , iff(response.response_code is null, null, response.is_non_score_response)
            as is_assessment_response_non_score
        , case
            when v.score_text is null then 'value_missing'
            when response.is_non_score_response then 'known_non_score'
            when response.response_code is not null then 'enumerated_response'
            when scale.numeric_range_count = 1
                and v.score_numeric_parsed between scale.minimum_numeric_value and scale.maximum_numeric_value
                then 'within_published_range'
            when v.score_numeric_parsed between earlier.minimum_numeric_value and earlier.maximum_numeric_value
                then 'historical_published_range'
            when scale.concept_code is null then 'reference_not_available'
            else 'response_unmatched'
        end as assessment_response_status
        , iff(assessment_response_status = 'historical_published_range', earlier.specification_version, null)
            as historical_reference_version
        -- Only values on the latest definition's scale are comparable; historical formats are not rescaled.
        , iff(assessment_response_status in ('enumerated_response', 'within_published_range')
            , coalesce(response.numeric_response_value, v.score_numeric_parsed), null) as assessment_score_numeric
        -- Extra decimals flow with a warning (ROM Mapping row 9), so they do not void the score.
        , iff(assessment_response_status = 'within_published_range'
            , coalesce(length(regexp_substr(v.score_text, '[.]([0-9]*[1-9])', 1, 1, 'e', 1)), 0)
                > coalesce(scale.decimal_places, 0)
            , null) as has_excess_score_precision

        , v.clinical_date
        , v.clinical_time
        , case
            when v.clinical_date is null then null
            when v.clinical_time is null then v.clinical_date::timestamp_ntz
            else timestamp_ntz_from_parts(v.clinical_date, v.clinical_time)
        end as clinical_at
        , case
            when v.clinical_date is null then 'unknown'
            when v.clinical_time is null then 'date'
            else 'timestamp'
        end as clinical_time_precision
        , iff(v.clinical_date is null, 'not_recorded', v.recorded_time_basis) as clinical_time_basis
        , v.clinical_date_status
        , v.source_derived_date
        , iff(v.clinical_date is null or v.source_derived_date is null, null,
            v.clinical_date <> v.source_derived_date) as is_source_date_inconsistent

        , v.referral_id
        , v.pathway_id
        , v.care_contact_id
        , v.care_activity_id
        , v.care_activity_source_record_id
        , v.is_care_activity_linked
        , v.is_care_activity_referral_consistent
        , v.is_care_activity_contact_consistent
        , v.is_care_activity_person_consistent

        , v.provider_organisation_code
        , provider.organisation_name as provider_organisation_name
        , v.reported_period_count
        , v.submission_id
        , v.source_row_id
        , v.reporting_period_start_date
        , v.reporting_period_end_date
        , v.unique_month_id
        , v.dataset_version
        , v.file_type
        , v.source_file_received_at
        , v.source_loaded_at
    from versions as v
    left join {{ ref('iapt_assessment_scale') }} as scale
        on v.assessment_tool_code = scale.concept_code
        and scale.is_latest_definition
    left join {{ ref('iapt_assessment_response') }} as response
        on scale.concept_code = response.concept_code
        and scale.specification_version = response.specification_version
        and (v.score_token = upper(response.response_code) or v.score_numeric_parsed = response.numeric_response_value)
    left join earlier_ranges as earlier
        on v.assessment_tool_code = earlier.concept_code
    left join {{ ref('snomed_concept') }} as snomed
        on v.assessment_tool_code = snomed.snomed_code
    left join {{ ref('stg_iapt_bridging') }} as b
        on v.person_id = b.person_id
    left join {{ ref('organisation') }} as provider
        on v.provider_organisation_code = provider.organisation_code
)

-- Parent facts carry their own parent_record_* columns, so every reference here is qualified.
, parent_choice as (
    select
        s.*
        , iff(s.referral_id is null, null, r.source_record_id is not null) as is_referral_linked
        -- Null when either Person_ID is missing: absence is not evidence of the same person.
        , case
            when r.source_record_id is null or s.person_id is null or r.person_id is null then null
            else s.person_id = r.person_id
        end as is_referral_person_consistent
        -- A parent is published only when the published parent row names the same non-null person
        -- and the same recorded ids; the same-submission activity must also agree.
        , case
            when s.is_care_activity_linked and s.is_care_activity_referral_consistent
                and s.is_care_activity_contact_consistent and s.is_care_activity_person_consistent
                and s.person_id = pa.person_id
                and s.referral_id = pa.referral_id
                and s.care_contact_id = pa.care_contact_id
                and s.care_activity_id = pa.care_activity_id
                then 'care_activity'
            when s.person_id = r.person_id and s.referral_id = r.referral_id then 'referral'
        end as parent_record_type
    from scored as s
    left join {{ ref('fct_iapt_referral') }} as r
        on s.referral_id = r.source_record_id
    left join {{ ref('fct_iapt_care_activity') }} as pa
        on s.care_activity_source_record_id = pa.source_record_id
)

-- One source in scope: id and model follow the single chosen parent_record_type.
select
    *
    , case parent_record_type
        when 'care_activity' then care_activity_source_record_id
        when 'referral' then referral_id
    end as parent_record_id
    , case parent_record_type
        when 'care_activity' then 'fct_iapt_care_activity'
        when 'referral' then 'fct_iapt_referral'
    end as parent_model_name
from parent_choice

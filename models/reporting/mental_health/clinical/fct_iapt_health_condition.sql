with conditions as (
    select
        'IDS601' as source_table
        , 'previous_diagnosis' as condition_record_type
        , submission_id
        , source_row_id
        , provider_organisation_code
        , person_id
        , null::varchar as referral_id
        , null::varchar as pathway_id
        , unique_local_patient_id as parent_key
        , diag_scheme_in_use as coding_scheme_code
        , case diag_scheme_in_use when '02' then 'ICD-10' when '06' then 'SNOMED CT' end as coding_system
        , prev_diag as submitted_code
        , null::varchar as nhsd_validated_code
        , diag_date as source_date
        , null::varchar as presenting_complaint_significance_code
        , reporting_period_start_date
        , reporting_period_end_date
        , unique_month_id
        , dataset_version
        , file_type
        , source_file_received_at
        , source_loaded_at
    from {{ ref('stg_iapt_previous_diagnosis_history') }}

    union all

    select
        'IDS602'
        , 'long_term_condition'
        , submission_id
        , source_row_id
        , provider_organisation_code
        , person_id
        , referral_id
        , pathway_id
        , referral_id
        , find_scheme_in_use
        , case find_scheme_in_use when '01' then 'ICD-10' when '04' then 'SNOMED CT' end
        , long_term_condition
        , validated_long_term_condition_code
        , null::date
        , null::varchar
        , reporting_period_start_date
        , reporting_period_end_date
        , unique_month_id
        , dataset_version
        , file_type
        , source_file_received_at
        , source_loaded_at
    from {{ ref('stg_iapt_long_term_condition_history') }}

    union all

    select
        'IDS603'
        , 'presenting_complaint'
        , submission_id
        , source_row_id
        , provider_organisation_code
        , person_id
        , referral_id
        , pathway_id
        , referral_id
        , find_scheme_in_use
        , case find_scheme_in_use when '01' then 'ICD-10' when '04' then 'SNOMED CT' end
        , pres_comp
        , validated_presenting_complaint
        , pres_comp_date
        , pres_comp_cod_sig
        , reporting_period_start_date
        , reporting_period_end_date
        , unique_month_id
        , dataset_version
        , file_type
        , source_file_received_at
        , source_loaded_at
    from {{ ref('stg_iapt_presenting_complaint_history') }}
)

, identified as (
    -- Supersession keys from ETOS v2.1.22 Complex Derivations rows 16-18, using the submitted code
    -- as sent: punctuation or a dagger/asterisk marker can distinguish submitted conditions. The
    -- volatile Person_ID is replaced by the provider-qualified local patient id for IDS601, the
    -- identity its duplicate validation uses (IDS601 row 1). A null date is a key value.
    select
        *
        , iff(coding_system = 'ICD-10', {{ iapt_icd10_lookup_code('submitted_code') }}, null)
            as submitted_code_normalised
        -- NHSD pads three-character codes with X or 9 and marks unmatched codes -3; IDS601 has no validated code.
        , iff(
            coding_system = 'ICD-10' and coalesce(nhsd_validated_code, '') <> '-3'
            , {{ iapt_icd10_lookup_code('coalesce(nhsd_validated_code, submitted_code)') }}, null
        ) as icd10_lookup_code
        , {{ dbt_utils.generate_surrogate_key([
            'source_table', 'provider_organisation_code', 'parent_key', 'coding_scheme_code',
            'submitted_code', 'source_date', 'incomplete_key_source_row_id'
        ]) }} as source_record_id
    from (
        select
            *
            -- The parent and code are mandatory; a row missing either keeps its own identity.
            , iff(parent_key is null or submitted_code is null, source_row_id, null) as incomplete_key_source_row_id
        from conditions
    )
)

, versions as (
    select
        *
        , min(reporting_period_end_date) over (partition by source_record_id) as first_reported_period_end_date
        , max(reporting_period_end_date) over (partition by source_record_id) as last_reported_period_end_date
        , count(distinct reporting_period_end_date) over (partition by source_record_id) as reported_period_count
        , min(person_id) over (partition by source_record_id)
            is distinct from max(person_id) over (partition by source_record_id) as has_person_identifier_changed
    from identified
    qualify row_number() over (
        partition by source_record_id
        order by unique_month_id desc nulls last, source_file_received_at desc nulls last,
            try_to_number(submission_id) desc nulls last, try_to_number(source_row_id) desc nulls last,
            submission_id desc, source_row_id desc
    ) = 1
)

-- ETOS v2.1.22 IDS603 row 24: an undated complaint is superseded when a later period sends the
-- same referral, scheme and code with a date.
, dated_complaints as (
    select
        parent_key
        , coding_scheme_code
        , submitted_code
        , min(first_reported_period_end_date) as first_dated_period_end_date
    from versions
    where source_table = 'IDS603' and source_date is not null
    group by parent_key, coding_scheme_code, submitted_code
)

, labelled as (
select
    v.source_record_id
    , 'IAPT' as source_dataset
    , v.source_table
    , v.condition_record_type
    , v.person_id
    , b.sk_patient_id
    , v.has_person_identifier_changed
    , v.referral_id
    , v.pathway_id

    , v.coding_scheme_code
    , coalesce(diagnosis_scheme.description, finding_scheme.description) as coding_scheme_description
    , v.coding_system
    , v.submitted_code
    , v.nhsd_validated_code
    , iff(v.coding_system = 'SNOMED CT', v.submitted_code, null) as snomed_code
    , iff(v.coding_system = 'ICD-10', {{ iapt_icd10_precision('v.submitted_code_normalised') }}, null)
        as icd10_precision
    , iff(
        v.coding_system = 'ICD-10' and v.nhsd_validated_code is not null
        , coalesce(length(v.submitted_code_normalised) = 3 and length(v.icd10_lookup_code) = 4
            and startswith(v.icd10_lookup_code, v.submitted_code_normalised), false)
        , null
    ) as is_code_extended_by_nhsd
    , left(v.icd10_lookup_code, 3) as icd10_category_code
    , icd10_category.description as icd10_category_description
    -- Three-character submissions keep the category label, not NHSD's padded "unspecified" code.
    , case v.coding_system
        when 'ICD-10' then iff(
            coalesce(is_code_extended_by_nhsd, false)
                or icd10_precision in ('three_character', 'three_character_filler')
            , coalesce(icd10_category.description, icd10.description)
            , icd10.description
        )
        when 'SNOMED CT' then snomed.preferred_term
    end as code_description
    , case
        when v.submitted_code is null then 'code_missing'
        when v.coding_scheme_code is null then 'coding_scheme_missing'
        when v.coding_system is null then 'coding_scheme_unrecognised'
        when v.coding_system = 'ICD-10' and v.nhsd_validated_code = '-3' then 'invalid_code_supplied'
        when code_description is not null then 'labelled'
        else 'code_unmatched'
    end as code_label_status
    , v.presenting_complaint_significance_code
    , significance.description as presenting_complaint_significance_description

    , case
        when v.source_table = 'IDS602' then 'not_collected'
        when v.source_date is null then 'missing'
        -- Project source-epoch rule: earlier dates are placeholders.
        when v.source_date < '1901-01-01'::date then 'source_sentinel'
        else 'recorded'
    end as clinical_date_status
    , iff(clinical_date_status = 'recorded', v.source_date, null) as clinical_date
    -- Midnight is a sort anchor; conditions carry dates only.
    , clinical_date::timestamp_ntz as clinical_at
    , iff(clinical_date is null, 'unknown', 'date') as clinical_time_precision
    , case
        when clinical_date is null then 'not_recorded'
        when v.source_table = 'IDS601' then 'previous_diagnosis_observed'
        else 'presenting_complaint_recorded'
    end as clinical_time_basis
    , iff(v.source_table = 'IDS603' and v.source_date is null
        , coalesce(dated.first_dated_period_end_date > v.first_reported_period_end_date, false), null)
        as is_superseded_by_dated_record

    , v.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , v.first_reported_period_end_date
    , v.last_reported_period_end_date
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
left join dated_complaints as dated
    on v.source_table = 'IDS603'
    and v.source_date is null
    and v.parent_key = dated.parent_key
    and v.coding_scheme_code is not distinct from dated.coding_scheme_code
    and v.submitted_code = dated.submitted_code
left join {{ ref('diagnosis_scheme') }} as diagnosis_scheme
    on v.source_table = 'IDS601'
    and v.coding_scheme_code = diagnosis_scheme.code
left join {{ ref('mhsds_care_activity_code_lookup') }} as finding_scheme
    on v.source_table <> 'IDS601'
    and finding_scheme.code_set_name = 'finding_scheme'
    and v.coding_scheme_code = finding_scheme.code
left join {{ ref('icd10_code') }} as icd10
    on v.icd10_lookup_code = icd10.code
left join {{ ref('icd10_code') }} as icd10_category
    on left(v.icd10_lookup_code, 3) = icd10_category.code
left join {{ ref('snomed_concept') }} as snomed
    on v.coding_system = 'SNOMED CT'
    and v.submitted_code = snomed.snomed_code
left join {{ ref('iapt_code_lookup') }} as significance
    on significance.code_set_name = 'coding_significance'
    and v.presenting_complaint_significance_code = significance.code
left join {{ ref('stg_iapt_bridging') }} as b
    on v.person_id = b.person_id
left join {{ ref('organisation') }} as provider
    on v.provider_organisation_code = provider.organisation_code
)

-- Every reference is qualified: the referral fact is joined only here.
, parent_choice as (
    select
        l.*
        , iff(l.referral_id is null, null, r.source_record_id is not null) as is_referral_linked
        -- Null when either Person_ID is missing: absence is not evidence of the same person.
        , case
            when r.source_record_id is null or l.person_id is null or r.person_id is null then null
            else l.person_id = r.person_id
        end as is_referral_person_consistent
        -- A parent is published only when the published referral row names the same non-null person.
        , iff(l.person_id = r.person_id and l.referral_id = r.referral_id, l.referral_id, null)
            as parent_record_id
    from labelled as l
    left join {{ ref('fct_iapt_referral') }} as r
        on l.referral_id = r.source_record_id
)

-- One source in scope: type and model follow parent_record_id.
select
    *
    , iff(parent_record_id is not null, 'referral', null) as parent_record_type
    , iff(parent_record_id is not null, 'fct_iapt_referral', null) as parent_model_name
from parent_choice

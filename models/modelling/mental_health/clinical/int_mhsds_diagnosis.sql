with diagnosis_records as (
    select
        'MHS601' as source_table
        , 'previous_diagnosis' as clinical_record_type
        , mhs601_uniq_id::varchar as source_row_id
        , local_patient_id as source_parent_id
        , null::varchar as referral_source_record_id
        , person_id
        , coded_diag_timestamp as source_timestamp
        , diag_date::date as source_derived_date
        , diag_scheme_in_use as diagnosis_scheme_code
        , prev_diag as clinical_code
        , master_snomed_ct_prev_diag_code as standardised_snomed_code
        , org_id_prov as provider_organisation_code
        , uniq_submission_id
        , reporting_period_start_date::date as reporting_period_start_date
        , reporting_period_end_date::date as reporting_period_end_date
        , dmic_dataset as mhsds_version
        , effective_from as source_file_received_at
        , dmic_date_added as source_loaded_at
        , record_number
        , row_number as source_row_number
    from {{ ref('stg_mhsds_previous_diagnosis') }}
    
    union all
    
    select
        'MHS603' as source_table
        , 'provisional_diagnosis' as clinical_record_type
        , mhs603_uniq_id::varchar as source_row_id
        , uniq_serv_req_id as source_parent_id
        , uniq_serv_req_id as referral_source_record_id
        , person_id
        , coded_prov_diag_timestamp as source_timestamp
        , prov_diag_date::date as source_derived_date
        , diag_scheme_in_use as diagnosis_scheme_code
        , prov_diag as clinical_code
        , master_snomed_ct_prov_diag_code as standardised_snomed_code
        , org_id_prov as provider_organisation_code
        , uniq_submission_id
        , reporting_period_start_date::date as reporting_period_start_date
        , reporting_period_end_date::date as reporting_period_end_date
        , dmic_dataset as mhsds_version
        , effective_from as source_file_received_at
        , dmic_date_added as source_loaded_at
        , record_number
        , row_number as source_row_number
    from {{ ref('stg_mhsds_provisional_diagnosis') }}
    
    union all
    
    select
        'MHS604' as source_table
        , 'primary_diagnosis' as clinical_record_type
        , mhs604_uniq_id::varchar as source_row_id
        , uniq_serv_req_id as source_parent_id
        , uniq_serv_req_id as referral_source_record_id
        , person_id
        , coded_diag_timestamp as source_timestamp
        , diag_date::date as source_derived_date
        , diag_scheme_in_use as diagnosis_scheme_code
        , prim_diag as clinical_code
        , master_snomed_ct_prim_diag_code as standardised_snomed_code
        , org_id_prov as provider_organisation_code
        , uniq_submission_id
        , reporting_period_start_date::date as reporting_period_start_date
        , reporting_period_end_date::date as reporting_period_end_date
        , dmic_dataset as mhsds_version
        , effective_from as source_file_received_at
        , dmic_date_added as source_loaded_at
        , record_number
        , row_number as source_row_number
    from {{ ref('stg_mhsds_primdiag') }}
    
    union all
    
    select
        'MHS605' as source_table
        , 'secondary_diagnosis' as clinical_record_type
        , mhs605_uniq_id::varchar as source_row_id
        , uniq_serv_req_id as source_parent_id
        , uniq_serv_req_id as referral_source_record_id
        , person_id
        , coded_diag_timestamp as source_timestamp
        , diag_date::date as source_derived_date
        , diag_scheme_in_use as diagnosis_scheme_code
        , sec_diag as clinical_code
        , master_snomed_ct_sec_diag_code as standardised_snomed_code
        , org_id_prov as provider_organisation_code
        , uniq_submission_id
        , reporting_period_start_date::date as reporting_period_start_date
        , reporting_period_end_date::date as reporting_period_end_date
        , dmic_dataset as mhsds_version
        , effective_from as source_file_received_at
        , dmic_date_added as source_loaded_at
        , record_number
        , row_number as source_row_number
    from {{ ref('stg_mhsds_secondary_diagnosis') }}
)

, dated as (
    select
        *
        -- Keep source epochs as evidence, not a usable diagnosis time or revision key.
        , coalesce(
            iff(source_timestamp::date >= '1901-01-01'::date, source_timestamp, null)
            , iff(source_derived_date >= '1901-01-01'::date,
                source_derived_date::timestamp_ntz, null)
        ) as clinical_at
        , case
            when source_timestamp::date >= '1901-01-01'::date then 'stored_timestamp_precision_unknown'
            when source_derived_date >= '1901-01-01'::date then 'date'
        end as clinical_time_precision
        , case
            when source_table = 'MHS601' then 'previous_diagnosis_discussion'
            else 'recorded_diagnosis'
        end as clinical_time_basis
        , coalesce(source_timestamp::date <> source_derived_date, false)
            as is_source_date_inconsistent
        , iff(
            clinical_at is null or source_parent_id is null
                or clinical_code is null or diagnosis_scheme_code is null
            , source_row_id, null
        ) as incomplete_key_source_row_id
    from diagnosis_records
)

, identified as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'source_table', 'provider_organisation_code', 'source_parent_id',
            'person_id', 'diagnosis_scheme_code', 'clinical_code', 'clinical_at',
            'clinical_time_precision', 'incomplete_key_source_row_id'
        ]) }} as source_record_id
        , count(distinct person_id) over (
            partition by source_table, provider_organisation_code, source_parent_id,
                diagnosis_scheme_code, clinical_code, clinical_at, clinical_time_precision
        ) > 1 as has_person_identifier_changed
    from dated
)

select
    * exclude (source_parent_id, incomplete_key_source_row_id, record_number, source_row_number)
    , iff(source_table = 'MHS601', source_parent_id, null) as local_patient_id
    , min(reporting_period_end_date) over (partition by source_record_id)
        as first_reported_period_end_date
    , max(reporting_period_end_date) over (partition by source_record_id)
        as last_reported_period_end_date
    , count(*) over (partition by source_record_id) as accepted_source_record_count
    , count(distinct reporting_period_end_date) over (partition by source_record_id)
        as reported_period_count
from identified
qualify row_number() over (
    partition by source_record_id
    order by reporting_period_end_date desc, source_file_received_at desc nulls last,
        uniq_submission_id desc, record_number desc, source_row_number desc,
        source_row_id desc
) = 1

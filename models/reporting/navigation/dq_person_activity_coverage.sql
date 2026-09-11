{{ config(tags=['daily', 'person_activity_coverage']) }}

with coverage as (
    select
        'healthcare_event'::varchar as output_type,
        source_dataset,
        source_record_type,
        count(*) as record_count,
        count_if(sk_patient_id is null) as unlinked_record_count,
        count_if(provider_organisation_code is null) as missing_provider_record_count,
        count_if(event_code is not null) as coded_record_count,
        count_if(event_code is not null and event_code_name is null) as unlabelled_code_record_count,
        count_if(event_date is null) as undated_record_count,
        count_if(event_time_precision = 'date') as date_only_record_count,
        count_if(event_time_precision in ('month', 'year')) as partial_date_record_count,
        count_if(event_time_precision = 'unknown') as unknown_precision_record_count,
        min(event_date) as first_record_date,
        max(event_date) as last_record_date,
        max(source_received_at) as latest_source_received_at
    from {{ ref('fct_person_healthcare_event') }}
    group by source_dataset, source_record_type

    union all

    select
        'clinical_record'::varchar as output_type,
        source_dataset,
        source_record_type,
        count(*) as record_count,
        count_if(sk_patient_id is null) as unlinked_record_count,
        count_if(provider_organisation_code is null) as missing_provider_record_count,
        count_if(source_code is not null) as coded_record_count,
        count_if(source_code is not null and source_code_name is null) as unlabelled_code_record_count,
        count_if(clinical_record_date is null) as undated_record_count,
        count_if(clinical_time_precision = 'date') as date_only_record_count,
        count_if(clinical_time_precision in ('month', 'year')) as partial_date_record_count,
        count_if(clinical_time_precision = 'unknown') as unknown_precision_record_count,
        min(clinical_record_date) as first_record_date,
        max(clinical_record_date) as last_record_date,
        max(source_received_at) as latest_source_received_at
    from {{ ref('fct_person_clinical_record') }}
    group by source_dataset, source_record_type
)
select
    output_type,
    source_dataset,
    source_record_type,
    record_count,
    unlinked_record_count,
    missing_provider_record_count,
    coded_record_count,
    unlabelled_code_record_count,
    undated_record_count,
    date_only_record_count,
    partial_date_record_count,
    unknown_precision_record_count,
    first_record_date,
    last_record_date,
    latest_source_received_at,
    current_timestamp()::timestamp_ntz as profiled_at
from coverage

{{ config(tags=['daily', 'person_activity_coverage']) }}

{# Coverage needs source counts, not the ordering in the analyst views. #}
{% set sources = [
    ('healthcare_event', 'int_mhsds_healthcare_event', 'event_date', 'event_time_precision', 'null', 'null'),
    ('healthcare_event', 'int_csds_healthcare_event', 'event_date', 'event_time_precision', 'null', 'null'),
    ('healthcare_event', 'int_olids_healthcare_event', 'event_date', 'event_time_precision', 'event_code', 'event_code_name'),
    ('healthcare_event', 'int_sus_healthcare_event', 'event_date', 'event_time_precision', 'null', 'null'),
    ('healthcare_event', 'int_ers_healthcare_event', 'event_date', 'event_time_precision', 'event_code', 'event_code_name'),
    ('clinical_record', 'int_mhsds_person_clinical_record', 'clinical_record_date', 'clinical_time_precision', 'source_code', 'source_code_name'),
    ('clinical_record', 'int_csds_person_clinical_record', 'clinical_record_date', 'clinical_time_precision', 'source_code', 'source_code_name'),
    ('clinical_record', 'int_olids_person_clinical_record', 'clinical_record_date', 'clinical_time_precision', 'source_code', 'source_code_name'),
    ('clinical_record', 'int_sus_person_clinical_record', 'clinical_record_date', 'clinical_time_precision', 'source_code', 'source_code_name'),
    ('clinical_record', 'int_ecds_person_clinical_record', 'clinical_record_date', 'clinical_time_precision', 'source_code', 'source_code_name')
] %}

with coverage as (
    {% for output_type, model_name, date_column, precision_column, code_column, label_column in sources %}
    select
        '{{ output_type }}'::varchar as output_type,
        source_dataset,
        source_record_type,
        count(*) as record_count,
        count_if(sk_patient_id is null) as unlinked_record_count,
        count_if(provider_organisation_code is null) as missing_provider_record_count,
        count_if({{ code_column }} is not null) as coded_record_count,
        count_if({{ code_column }} is not null and {{ label_column }} is null) as unlabelled_code_record_count,
        count_if({{ date_column }} is null) as undated_record_count,
        count_if({{ precision_column }} = 'date') as date_only_record_count,
        count_if({{ precision_column }} in ('month', 'year')) as partial_date_record_count,
        count_if({{ precision_column }} = 'unknown') as unknown_precision_record_count,
        min({{ date_column }}) as first_record_date,
        max({{ date_column }}) as last_record_date,
        max(source_received_at) as latest_source_received_at
    from {{ ref(model_name) }}
    group by source_dataset, source_record_type
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
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

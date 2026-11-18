{{ config(materialized='table') }}

-- Each source family is reduced to person grain before the union.
{% set families = [
    ('patient_record', 'stg_csds_mpi_history', 'reporting_period_end_date'),
    ('gp_registration', 'stg_csds_gp_registration_history', 'reporting_period_end_date'),
    ('referral', 'stg_csds_referral_history', 'reporting_period_end_date'),
    ('service_team', 'stg_csds_service_type_history', 'reporting_period_end_date'),
    ('referral_to_treatment', 'stg_csds_referral_to_treatment_history', 'reporting_period_end_date'),
    ('care_contact', 'stg_csds_care_contact_history', 'reporting_period_end_date'),
    ('care_activity', 'stg_csds_care_activity_history', 'reporting_period_end_date'),
] %}

{% for evidence_type, model, period_column in families %}
select
    person_id
    , '{{ evidence_type }}' as evidence_type
    , count(*) as n_records
    , min({{ period_column }})::date as first_reporting_period_end_date
    , max({{ period_column }})::date as last_reporting_period_end_date
from {{ ref(model) }}
where person_id is not null
group by person_id
union all
{% endfor %}
select
    person_id
    , 'clinical_record' as evidence_type
    , count(*) as n_records
    , min(first_reported_period_end_date) as first_reporting_period_end_date
    , max(last_reported_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_csds_clinical_record') }}
where person_id is not null
group by person_id

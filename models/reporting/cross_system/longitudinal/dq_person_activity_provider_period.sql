{{ config(tags=['person_activity_coverage', 'monthly-full']) }}

-- Provider activity and submission evidence, not certified period completeness.
with bounds as (
    select date_trunc('month', current_date())::date as end_month,
        dateadd(month, -24, end_month)::date as start_month
), events as (
    {% for model_name in ['int_mhsds_healthcare_event', 'int_csds_healthcare_event',
        'int_olids_healthcare_event', 'int_sus_healthcare_event', 'int_ers_healthcare_event'] %}
    select source_dataset, provider_code_authority, provider_organisation_code,
        date_trunc('month', event_date)::date as activity_month,
        count(*) as event_record_count,
        coalesce(count_if(sk_patient_id is null), 0) as unlinked_event_record_count,
        coalesce(count_if(event_type in ('appointment_booking', 'appointment_rebooked', 'appointment_slot')), 0)
            as scheduled_milestone_record_count,
        min(event_date) as first_event_date,
        max(event_date) as last_event_date,
        max(source_received_at) as latest_source_received_at
    from {{ ref(model_name) }}
    cross join bounds
    where event_date >= bounds.start_month and event_date < bounds.end_month
        and event_time_precision in ('date', 'timestamp')
    group by source_dataset, provider_code_authority, provider_organisation_code, activity_month
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
), submissions as (
    {% for dataset, model_name in [('MHSDS', 'dq_mhsds_provider_submission'), ('CSDS', 'dq_csds_provider_submission')] %}
    select '{{ dataset }}'::varchar as source_dataset,
        'ODS'::varchar as provider_code_authority, provider_organisation_code,
        date_trunc('month', reporting_period_end_date)::date as activity_month,
        sum(n_accepted_submissions) as accepted_submission_count,
        sum(n_contact_source_records) as submitted_contact_record_count,
        max(provider_latest_reporting_period_end_date)::date as provider_latest_submission_period,
        max(dataset_latest_reporting_period_end_date)::date as dataset_latest_submission_period
    from {{ ref(model_name) }}
    group by provider_organisation_code, activity_month
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
), provider_latest as (
    select source_dataset, provider_code_authority, provider_organisation_code,
        max(provider_latest_submission_period) as provider_latest_submission_period
    from submissions
    group by source_dataset, provider_code_authority, provider_organisation_code
), dataset_latest as (
    select source_dataset, max(dataset_latest_submission_period) as dataset_latest_submission_period
    from submissions
    group by source_dataset
), observed_months as (
    select source_dataset, provider_code_authority, provider_organisation_code, activity_month
    from events
    union
    select s.source_dataset, s.provider_code_authority, s.provider_organisation_code, s.activity_month
    from submissions as s cross join bounds
    where s.accepted_submission_count > 0
        and s.activity_month >= bounds.start_month and s.activity_month < bounds.end_month
), provider_bounds as (
    select source_dataset, provider_code_authority, provider_organisation_code,
        min(activity_month) as first_month, max(activity_month) as last_month
    from observed_months
    group by source_dataset, provider_code_authority, provider_organisation_code
), months as (
    select dateadd(month, row_number() over (order by seq4()) - 1, bounds.start_month)::date as activity_month
    from table(generator(rowcount => 24)) cross join bounds
), provider_months as (
    select p.source_dataset, p.provider_code_authority, p.provider_organisation_code, m.activity_month
    from provider_bounds as p
    join months as m on m.activity_month between p.first_month and p.last_month
)
select p.source_dataset, p.provider_code_authority, p.provider_organisation_code, p.activity_month,
    coalesce(e.event_record_count, 0) as event_record_count,
    coalesce(e.unlinked_event_record_count, 0) as unlinked_event_record_count,
    coalesce(e.scheduled_milestone_record_count, 0) as scheduled_milestone_record_count,
    e.first_event_date, e.last_event_date, e.latest_source_received_at,
    iff(p.source_dataset in ('MHSDS', 'CSDS'), coalesce(s.accepted_submission_count, 0), null)
        as accepted_submission_count,
    l.provider_latest_submission_period,
    d.dataset_latest_submission_period,
    datediff(month, l.provider_latest_submission_period, d.dataset_latest_submission_period)
        as provider_months_behind_dataset,
    iff(p.source_dataset in ('MHSDS', 'CSDS'), coalesce(s.submitted_contact_record_count, 0), null)
        as submitted_contact_record_count,
    case
        when s.accepted_submission_count > 0 then 'accepted_submission_observed'
        when p.source_dataset in ('MHSDS', 'CSDS') then 'no_accepted_submission_observed'
        else 'submission_completeness_unknown'
    end::varchar as coverage_status,
    current_timestamp()::timestamp_ntz as profiled_at
from provider_months as p
left join events as e
    on p.source_dataset = e.source_dataset
    and equal_null(p.provider_code_authority, e.provider_code_authority)
    and equal_null(p.provider_organisation_code, e.provider_organisation_code)
    and p.activity_month = e.activity_month
left join submissions as s
    on p.source_dataset = s.source_dataset
    and equal_null(p.provider_code_authority, s.provider_code_authority)
    and equal_null(p.provider_organisation_code, s.provider_organisation_code)
    and p.activity_month = s.activity_month
left join provider_latest as l
    on p.source_dataset = l.source_dataset
    and equal_null(p.provider_code_authority, l.provider_code_authority)
    and equal_null(p.provider_organisation_code, l.provider_organisation_code)
left join dataset_latest as d on p.source_dataset = d.source_dataset

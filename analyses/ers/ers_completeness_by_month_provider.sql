-- One row per action month, plus provider/month groups with at least 1,000 actions.
-- The threshold limits diagnostic detail; it does not alter any fact population.
with coverage as (
    select
        date_trunc('month', action_at)::date as action_month,
        iff(grouping(provider_organisation_code) = 1, 'all_providers', 'provider') as profile_level,
        provider_organisation_code,
        count(*) as action_rows,
        count(sk_patient_id) as patient_key_rows,
        count(action_code) as action_code_rows,
        count_if(action_code is not null and action_name is null) as unlabelled_action_rows,
        count(action_reason_code) as reason_rows,
        count_if(action_reason_code is not null and action_reason_name is null) as unlabelled_reason_rows,
        count(pathway_started_at) as pathway_start_rows,
        count(referring_organisation_code) as referrer_rows,
        count(action_organisation_code) as action_organisation_rows,
        count(provider_organisation_code) as provider_rows,
        count(service_id) as service_rows,
        count_if(service_id is not null and service_name is null) as unlabelled_service_rows,
        count_if(service_id is not null and provider_organisation_code is null) as service_without_provider_rows,
        count_if(service_id is not null and service_specialty_code is null) as service_without_specialty_rows,
        count(specialty_code) as search_specialty_rows,
        count(appointment_at) as appointment_time_rows,
        count_if(source_imported_at < action_at) as imported_before_action_rows,
        count_if(pathway_started_at > action_at) as pathway_after_action_rows,
        approx_percentile(datediff('day', action_at, source_imported_at), 0.5) as median_import_lag_days,
        approx_percentile(datediff('day', action_at, source_imported_at), 0.95) as p95_import_lag_days
    from {{ ref('fct_ers_referral_action') }}
    group by grouping sets ((action_month), (action_month, provider_organisation_code))
)
select *,
    round(100.0 * patient_key_rows / nullif(action_rows, 0), 3) as patient_key_pct,
    round(100.0 * service_rows / nullif(action_rows, 0), 3) as service_pct,
    round(100.0 * service_without_specialty_rows / nullif(service_rows, 0), 3) as service_missing_specialty_pct
from coverage
where profile_level = 'all_providers' or action_rows >= 1000
order by action_month, profile_level, provider_organisation_code

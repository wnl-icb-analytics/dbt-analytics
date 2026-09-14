-- Aggregate-only source checks. Compile and run the SQL in a Snowflake session.
with keys as (
    select 'referral' as source_entity, count(*) as occurrences,
        count(distinct person_id) as people,
        count(distinct reporting_period_end_date) as reporting_periods
    from {{ ref('stg_csds_referral_history') }}
    group by unique_service_request_identifier
    union all
    select 'contact', count(*), count(distinct person_id), count(distinct reporting_period_end_date)
    from {{ ref('stg_csds_care_contact_history') }}
    group by unique_service_request_identifier, unique_care_contact_identifier
)
select source_entity, count(*) as logical_keys, sum(occurrences) as accepted_rows,
    count_if(people > 1) as keys_with_changed_person,
    count_if(reporting_periods > 1) as keys_in_multiple_months
from keys
group by source_entity

with population as (
    select distinct person_id
    from {{ ref('int_mhsds_person_evidence') }}
)
, missing_or_extra_people as (
    select count(*) as failures
    from population as p
    full outer join {{ ref('fct_mhsds_person_summary') }} as s
        on p.person_id = s.person_id
    where p.person_id is null or s.person_id is null
)
, invalid_measures as (
    select count(*) as failures
    from {{ ref('fct_mhsds_person_summary') }}
    where latest_contact_date > as_of_date
        or latest_attended_contact_date > as_of_date
        or n_attended_contacts_12m > n_contacts_12m
        or n_undated_primary_diagnosis_records > n_primary_diagnosis_records
        or (latest_primary_diagnosis_recorded_at is null and n_latest_primary_diagnosis_records <> 0)
        or (latest_primary_diagnosis_recorded_at is not null and n_latest_primary_diagnosis_records = 0)
)
select 'population' as rule, failures from missing_or_extra_people where failures > 0
union all
select 'measure_semantics' as rule, failures from invalid_measures where failures > 0

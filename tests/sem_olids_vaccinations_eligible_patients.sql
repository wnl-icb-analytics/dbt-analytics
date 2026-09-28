{{ config(static_analysis='off') }}

-- Risk groups can repeat a person; vaccination-only records are not eligible.
with expected as (
    select
        programme_type,
        campaign_id,
        count(distinct case when is_eligible then person_id end) as eligible_patients
    from {{ ref('fct_covid_flu_uptake') }}
    group by programme_type, campaign_id
),

actual as (
    select
        programme_type,
        campaign_id,
        agg(eligible_patients) as eligible_patients
    from {{ ref('sem_olids_vaccinations') }}
    group by programme_type, campaign_id
)

select
    coalesce(e.programme_type, a.programme_type) as programme_type,
    coalesce(e.campaign_id, a.campaign_id) as campaign_id,
    e.eligible_patients as expected_eligible_patients,
    a.eligible_patients as actual_eligible_patients
from expected e
full outer join actual a
    on e.programme_type = a.programme_type
    and e.campaign_id = a.campaign_id
where not equal_null(e.eligible_patients, a.eligible_patients)

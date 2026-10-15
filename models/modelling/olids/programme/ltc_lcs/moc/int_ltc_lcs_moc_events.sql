{{ config(materialized='table', cluster_by=['person_id']) }}

-- One row per person, programme event code and date. Future events are excluded.
with local_observations as (
    {{ get_ltc_lcs_observations('06a67476-a82f-8c9e-d328-c0c7c5aea698', match_paths='source') }}
),
local_events as (
    select
        o.person_id,
        case ecm.source_code
            when '552991000006110' then 'C'
            when '13072931000006119' then 'A'
            when '13072951000006114' then 'R'
            when '13489791000006113' then 'D'
            when '13489801000006114' then 'O'
        end as event_code,
        o.clinical_effective_date::date as event_date
    from local_observations o
    inner join {{ ref('stg_olids_observation') }} obs on o.observation_id = obs.id
    inner join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on obs.observation_source_concept_id = ecm.source_concept_id
    where ecm.source_code in (
        '552991000006110', '13072931000006119', '13072951000006114',
        '13489791000006113', '13489801000006114'
    )
),
uk_observations as (
    {{ get_ltc_lcs_observations('50bf74e8-ae15-b5f1-0e25-f6155f398c47,1e4b1d73-eaed-7e8d-da5e-0a77ca874c17') }}
),
all_events as (
    select person_id, event_code, event_date from local_events
    union all
    select
        person_id,
        case valueset_id
            when '50bf74e8-ae15-b5f1-0e25-f6155f398c47' then 'M'
            when '1e4b1d73-eaed-7e8d-da5e-0a77ca874c17' then 'S'
        end as event_code,
        clinical_effective_date::date as event_date
    from uk_observations
)
select person_id, event_code, event_date
from all_events
where event_date <= current_date()
group by person_id, event_code, event_date

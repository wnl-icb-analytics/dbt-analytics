{{
    config(
        materialized='table',
        tags=['monthly', 'sus', 'ae']
    )
}}

--------------------------------------------------------------------------------
-- Care home residence at the time of each A&E attendance.
--
-- Grain: one row per attendance (sk_encounter_id). A patient may have several
-- care home spells covering the same arrival date, so a tie-break is needed.
--
-- TIE-BREAK RULE, most significant first:
--   1. latest period_start   -- the most recently begun spell wins
--   2. latest period_end     -- nulls first, i.e. an open spell beats a closed
--                               one that started the same day
--   3. lowest organisation_code -- arbitrary but stable, so the same row wins
--                                 on every run
--
-- Rule 3 exists only to guarantee determinism. max_by() was used previously and
-- resolved ties arbitrarily, so the chosen care home could change between runs
-- for the same input.
--------------------------------------------------------------------------------

select
    encounter.sk_encounter_id,
    organisation.organisation_code as care_home_code

from {{ ref('int_sus_ae_monthly') }} as encounter

inner join {{ ref('stg_fact_patient_factcarehome') }} as care_home
    on encounter.sk_patient_id = care_home.sk_patient_id
   and encounter.arrival_date between cast(care_home.period_start as date)
                                      and coalesce(cast(care_home.period_end as date), '2050-12-31'::date)

left join {{ ref('stg_dictionary_dbo_organisation') }} as organisation
    on care_home.sk_organisation_id = organisation.sk_organisation_id

qualify row_number() over (
    partition by encounter.sk_encounter_id
    order by
        care_home.period_start desc,
        care_home.period_end desc nulls first,
        organisation.organisation_code asc
) = 1
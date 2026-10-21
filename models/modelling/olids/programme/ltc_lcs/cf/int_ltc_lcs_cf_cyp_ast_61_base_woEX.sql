{{ config(materialized='table') }}

-- CYP_AST_61 candidates before screening exclusions. One row per person at build date.
-- Currently registered, living children aged 18 months to under 18 years, outside
-- ICS_RESP_LTC, whose latest AST_COD/ASTRES_COD event is not active asthma.
-- Include asthma products or drug groups issued strictly after 12 months ago and
-- before today, or prednisolone, montelukast, suspected asthma or viral induced
-- wheeze recorded strictly after one year ago. The latter arms have no upper bound.
-- XML drug groups have no expanded concepts. BNF 03.01/03.02/03.03 approximate
-- bronchodilators, inhaled corticosteroids and asthma prophylaxis, not all chapter 03.
-- XML authority: ICB_CF_CYPAST_61_BASE / _woEX, R5 updated 27 November 2025.
-- Library ee5b135f is absent from the export but is already within ICS_RESP_LTC
-- through the XML COPD register. The product and screening sets share a name, so
-- their warehouse value-set IDs are pinned separately.
-- The prednisolone constituent excludes the XML's rectal and injectable products
-- and their descendants. The other prednisolone lists remain independent alternatives.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_respiratory_excluding_condition = false
        -- age 18 months to 17 years inclusive (17 inclusive = under 18)
        and age_months >= 18
        and age_at_least < 18
),

-- _BASE carve (canon: "Exclude patients who match [ASTRES_COD UNION AST_COD] then Latest 1
-- where SNOMED code IN AST_COD"). AST_COD (be433490, active asthma codes) and ASTRES_COD
-- (e49a6f96, 'Asthma resolved') are DISJOINT. Take each patient's latest asthma event across
-- the UNION of both sets; REJECT when that latest event is an active AST_COD code (current
-- asthma activity). Keep patients whose latest asthma event is 'resolved', or who have none.
ast_union_events as (
    select person_id, observation_id, clinical_effective_date, valueset_id
    from ({{ get_ltc_lcs_observations("e49a6f96-d7a8-ad43-7448-2166a2858204, be433490-3609-4849-20b6-045ddfbdc032") }})
),

latest_ast_event as (
    select person_id, valueset_id
    from ast_union_events
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

active_asthma_latest as (
    -- latest asthma event is an active AST_COD code -> reject from the eligible base
    select person_id
    from latest_ast_event
    where valueset_id = 'be433490-3609-4849-20b6-045ddfbdc032'
),

base_carved as (
    select b.person_id
    from base as b
    where b.person_id not in (select person_id from active_asthma_latest)
),

prednisolone_exception_roots as (
    select translated_to_snomed_code as concept_code
    from {{ ref('stg_reference_ltc_lcs_exceptions') }}
    where valueset_id = 'f1fdb838-2556-257c-fd9d-c621ad7ea0f3'
),

prednisolone_exceptions as (
    select concept_code from prednisolone_exception_roots
    union
    select tc.sub_type_id::varchar as concept_code
    from {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} as tc
    inner join prednisolone_exception_roots as ex
        on tc.super_type_id::varchar = ex.concept_code
),

-- woEX eligible arms (any match in the relevant window)
eligible as (
    -- Any issue within the XML window qualifies, even when a newer issue is today.
    select person_id
    from ({{ get_ltc_lcs_medication_orders("54940f0f-b140-2393-9d8d-0104b3103367") }})
    where order_date > dateadd(month, -12, current_date())
        and order_date < current_date()
    union
    -- Unexpanded XML drug groups use their BNF sections, excluding allergy medicines.
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='03') }})
    where bnf_section in ('0301', '0302', '0303')
        and order_date > dateadd(month, -12, current_date())
        and order_date < current_date()
    union
    -- vs3/vs4/vs5 prednisolone (last 12 months / "last 1 year")
    select person_id
    from ({{ get_ltc_lcs_medication_orders("f1fdb838-2556-257c-fd9d-c621ad7ea0f3, 3ad6ed82-b887-ed90-a8e7-254979cf33c1, 48b40183-3fce-b10b-dcbe-3ef134f2426f") }}) as mo
    where order_date > dateadd(month, -12, current_date())
        and (
            mo.valueset_id != 'f1fdb838-2556-257c-fd9d-c621ad7ea0f3'
            or not exists (
                select 1 from prednisolone_exceptions as ex
                where ex.concept_code = mo.concept_code
            )
        )
    union
    -- vs6 montelukast (last 12 months / "last 1 year")
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("fbf51055-a383-65db-3bfe-7d5b3f508d1c") }})
    where order_date > dateadd(month, -12, current_date())
    union
    -- vs7 Suspected asthma (clinical code, last 12 months)
    select person_id
    from ({{ get_ltc_lcs_observations("c61ece91-03d9-3fc8-3ea9-39a98be4ad5a") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    -- vs8 Viral induced wheeze (clinical code, last 12 months; NO age>=6 gate per canon)
    select person_id
    from ({{ get_ltc_lcs_observations("52527265-508f-4c80-7d04-ef7605e8b3e0") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
)

select distinct bc.person_id
from base_carved as bc
inner join eligible as e
    on bc.person_id = e.person_id

{{ config(materialized='table') }}

-- AF_61 candidates before the final AF marker exclusion. One row per person at build date.
-- Currently registered, living patients outside ICS_METABOLIC_LTC and CF_NHSHC2Y.
-- Include current Automatic, Repeat or Repeat Dispensing courses of oral
-- anticoagulants, digoxin, flecainide, propafenone or cardiac glycosides, without a
-- course-start lookback. Separately include anticoagulant/protamine issues strictly
-- after six months ago, without a current-course or repeat-prescription restriction.
-- Unexpanded drug groups use BNF 02.08.02 (oral courses), 02.01.01 (glycosides)
-- and 02.08.02-02.08.03 (issues). These BNF mappings are not exact drug-group expansions.
-- The issue rule excludes parenteral anticoagulants (02.08.01) and reversal agents,
-- including idarucizumab and andexanet, as in the XML.
-- Exclude ever-recorded thrombosis/antiphospholipid, AF/flutter, Long COVID and
-- hypoplastic left heart evidence. AF_62 subtracts this whole candidate population.
-- XML authority: ICB_CF_AF_61_BASE / _base_woEX, R5 updated 27 November 2025.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

-- Keep both mapped and source-code matches for the XML ingredient list.
af_course_codes as (
    select medication_statement_id
    from ({{ get_ltc_lcs_medication_statements("98b8e1d3-7db0-0653-9412-2fdf795e6f61") }})
),

af_medication as (
    select ms.person_id
    from {{ ref('stg_olids_medication_statement') }} as ms
    where ms.is_active = true
        and ms.authorisation_type_source_code in (
            'Automatic', 'Repeat', 'Repeat Dispensing'
        )
        and (
            ms.bnf_code like '020802%'
            or ms.bnf_code like '020101%'
            or ms.id in (select medication_statement_id from af_course_codes)
        )
    union
    -- The issue arm is independent of course status and prescription type.
    select person_id
    from ({{ get_medication_orders(bnf_code='0208') }})
    where order_date > dateadd(month, -6, current_date())
        and substr(bnf_code, 1, 6) in ('020802', '020803')
),

-- woEX clinical exclusions (ever recorded)
woex_excluded as (
    -- vs1: 223-code DVT/PE/antiphospholipid set (name collides with the 2-code Rule-1 marker -> pin id)
    select person_id
    from ({{ get_ltc_lcs_observations("f4f774b1-0949-b062-b74a-d03f7d820a81", emis_local_exact=true) }})
    union
    -- vs2: atrial fibrillation / flutter (8 codes)
    select person_id
    from ({{ get_ltc_lcs_observations("08ba16a7-dfb1-1282-f9a6-288f2641f431") }})
    union
    -- vs3: Long COVID-19
    select person_id
    from ({{ get_ltc_lcs_observations("c7ea5a27-039f-d39d-bfbb-5a5c37240961") }})
    union
    -- vs4: Hypoplastic left heart syndrome (canon: include if NOT match -> exclude if match)
    select person_id
    from ({{ get_ltc_lcs_observations("8463a4a4-8199-6956-5458-a3014aaa8f11") }})
)

select distinct b.person_id
from base as b
inner join af_medication as m
    on b.person_id = m.person_id
where b.person_id not in (select person_id from woex_excluded)

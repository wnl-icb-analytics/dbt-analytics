{{ config(materialized='table') }}

-- AF_61 candidates before the final AF marker exclusion. One row per person at build date.
-- Currently registered, living patients outside ICS_METABOLIC_LTC and CF_NHSHC2Y.
-- Include current Automatic, Repeat or Repeat Dispensing cardiac glycoside courses,
-- without a course-start lookback. Separately include anticoagulant/protamine issues strictly
-- after six months ago, without a current-course or repeat-prescription restriction.
-- XML course criterion b799351b requires both Drug filters: oral anticoagulants AND
-- digoxin/flecainide/propafenone. No course meets both, so that criterion selects none.
-- Oral anticoagulant issues use the national OPENCODELISTS DOAC and vitamin K antagonist lists.
-- Glycosides (BNF 02.01.01) and protamine (02.08.03) await reviewed dm+d clusters.
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

national_oral_anticoagulant_codes as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where upper(cluster_id) in (
            'NHS_DRUG_REFSETS/DIRECTORANTICOAGDRUG_COD',
            'NHS_DRUG_REFSETS/VITKANTAGDRUG_COD'
        )
        and source = 'OPENCODELISTS'
),

-- The XML names these reversal preparations; exclude their dm+d roots and descendants.
reversal_product_roots as (
    select column1::varchar as concept_code
    from (values
        ('37454211000001101'), -- Andexanet alfa 200mg vials
        ('37347611000001103'), -- Ondexxya 200mg vials
        ('31141811000001100'), -- Idarucizumab 2.5g/50ml vials
        ('31065711000001108')  -- Praxbind 2.5g/50ml vials
    )
),

reversal_products as (
    select concept_code from reversal_product_roots
    union
    select tc.sub_type_id::varchar as concept_code
    from {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} as tc
    inner join reversal_product_roots as ex
        on tc.super_type_id::varchar = ex.concept_code
),

anticoagulant_issues as (
    select mo.person_id, mo.clinical_effective_date as order_date, mo.mapped_concept_code, mo.bnf_code
    from {{ ref('stg_olids_medication_order') }} as mo
    inner join national_oral_anticoagulant_codes as c
        on mo.mapped_concept_code = c.code
    union all
    -- Retain the protamine BNF route until a reviewed dm+d cluster is available.
    select person_id, order_date, mapped_concept_code, bnf_code
    from ({{ get_medication_orders(bnf_code='020803') }})
),

af_medication as (
    select ms.person_id
    from {{ ref('stg_olids_medication_statement') }} as ms
    where ms.is_active = true
        and ms.authorisation_type_source_code in (
            'Automatic', 'Repeat', 'Repeat Dispensing'
        )
        and ms.bnf_code like '020101%'
    union
    -- The issue arm is independent of course status and prescription type.
    select person_id
    from anticoagulant_issues as issue
    where issue.order_date > dateadd(month, -6, current_date())
        and coalesce(issue.bnf_code, '') not like '020801%'
        and not exists (
            select 1 from reversal_products as ex
            where ex.concept_code = issue.mapped_concept_code
        )
),

-- woEX clinical exclusions (ever recorded)
woex_excluded as (
    -- vs1: 223-code DVT/PE/antiphospholipid set (name collides with the 2-code Rule-1 marker -> pin id)
    select person_id
    from ({{ get_ltc_lcs_observations("f4f774b1-0949-b062-b74a-d03f7d820a81") }})
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

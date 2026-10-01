{{ config(materialized='table') }}

-- LTC LCS HF case finding: ICB_CF_HF_61_woEX population (shared parent, pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hf/icb-cf-hf-61.md
--
-- The pre-Rule-1 HF_61 population. Consumed by int_ltc_lcs_cf_hf_61, which adds the
-- final Rule 1 ("Heart failure excluded" in the last 3 years).
--
-- ICB_CF_HF_61_BASE: currently registered, NOT in ICS_METABOLIC_LTC (which already
--   contains the AF and HF registers), NOT on the AF/HF register (named separately in
--   canon, applied explicitly belt-and-braces), AND NOT carrying any HF-evidence code
--   eligible_for_hf_casefinding_vs1 (7 codes: HFrEF/CCF/CCFDN..., ever recorded).
--   CF_NHSHC2Y is NOT applied to HF (canon BASE does not list it).
-- ICB_CF_HF_61_woEX: canon = "REQUIRE Spironolactone (last 3 months) AND include patients
--   who match ANY of the arms below", then drop the PCOS-pattern carve-out (age < 40 AND
--   Female AND hirsutism). Spironolactone is a HARD GATE (canon keyword "Require"), ANDed
--   with the OR-group of the remaining inclusion arms.
--   Arms (canon woEX prose):
--     med_hf      : Sacubitril/Valsartan (vs1) OR Entresto (vs2) OR Ivabradine/Eplerenone
--                   (vs3), order in last 3 months
--     med_sglt2   : Dapagliflozin / Dapagliflozin Propanediol Monohydrate / Digoxin +1
--                   (vs4), order in last 3 months
--     bnp_400     : latest pro-BNP (vs5) numeric value > 400 (no date window)
--     bnp_2000    : pro-BNP (vs5) within last 2 years, then latest within that window,
--                   numeric value > 2000 (window FIRST, then latest)
--     symptom     : (cardiomyopathy etc vs8 OR HF-excluded vs7) latest 1 AND
--                   (breathlessness etc vs9 OR HF-excluded vs7) latest 1 -> presence in
--                   each pool (EMIS "Latest 1" = the latest pool record exists)
--     med_digoxin : Digoxin (vs10), order in last 3 months
--     med_spiro   : Spironolactone (vs11), order in last 3 months
--
-- Valueset notes:
--   vs1 friendly name COLLIDES (woEX Sacubitril/Valsartan id 299f9e3b... vs final-search
--     "HF excluded" id 9228a15e...). Here the woEX copy is the named friendly form, which
--     the macro resolves to the woEX id; the final marker id is pinned only in int_ltc_lcs_cf_hf_61.
--   vs2 (Entresto) and vs12 (Female) have no SNOMED expansion (0 mapped) but 1 source-path
--     code each, so the medication macro matches Entresto via the source path; vs12 (an
--     EMIS-internal gender attribute) is resolved from dim_person_gender, not the obs macro.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
),

af_register as (
    select distinct person_id
    from {{ ref('fct_person_atrial_fibrillation_register') }}
    where is_on_register = true
),

hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
    where is_on_register = true
),

-- BASE HF-evidence exclusion: any eligible_for_hf_casefinding_vs1 code, ever
hf_evidence as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("eligible_for_hf_casefinding_vs1") }})
),

-- Eligible parent population after BASE exclusions
hf_base as (
    select b.person_id
    from base as b
    where b.person_id not in (select person_id from af_register)
        and b.person_id not in (select person_id from hf_register)
        and b.person_id not in (select person_id from hf_evidence)
),

-- woEX inclusion arms ----------------------------------------------------------

-- med_hf: Sacubitril/Valsartan (vs1) OR Entresto (vs2) OR Ivabradine/Eplerenone (vs3), 3m
arm_med_hf as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs1, hf_case_finding_eligible_patients_vs2, hf_case_finding_eligible_patients_vs3") }})
    where order_date >= dateadd(month, -3, current_date())
),

-- med_sglt2: Dapagliflozin / Digoxin group (vs4), 3m
arm_med_sglt2 as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs4") }})
    where order_date >= dateadd(month, -3, current_date())
),

-- bnp_400: latest pro-BNP (vs5) where numeric value > 400 (no date window)
arm_bnp_400 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("hf_case_finding_eligible_patients_vs5") }})
    where result_value > 400
),

-- bnp_2000: pro-BNP (vs5) within last 2 years, then latest within window, value > 2000
arm_bnp_2000 as (
    select person_id
    from (
        select person_id, result_value
        from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs5") }})
        where clinical_effective_date >= dateadd(year, -2, current_date())
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    )
    where result_value > 2000
),

-- symptom: (vs8 OR vs7) present AND (vs9 OR vs7) present
symptom_pool_a as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs8, hf_case_finding_eligible_patients_vs7") }})
),

symptom_pool_b as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs9, hf_case_finding_eligible_patients_vs7") }})
),

arm_symptom as (
    select a.person_id
    from symptom_pool_a as a
    inner join symptom_pool_b as b
        on a.person_id = b.person_id
),

-- med_digoxin: Digoxin (vs10), 3m
arm_med_digoxin as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs10") }})
    where order_date >= dateadd(month, -3, current_date())
),

-- med_spiro: Spironolactone (vs11), 3m
arm_med_spiro as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs11") }})
    where order_date >= dateadd(month, -3, current_date())
),

-- Any inclusion arm (OR group). Spironolactone is NOT here -- it is the required gate.
included as (
    select person_id from arm_med_hf
    union
    select person_id from arm_med_sglt2
    union
    select person_id from arm_bnp_400
    union
    select person_id from arm_bnp_2000
    union
    select person_id from arm_symptom
    union
    select person_id from arm_med_digoxin
),

-- PCOS-pattern carve-out: age < 40 AND Female AND any hirsutism (vs13), applied to the
-- WHOLE woEX population. vs12 ("Female") is an EMIS-internal gender attribute with no
-- clinical-code series in OLIDS, so gender comes from dim_person_gender.
female_patients as (
    select distinct person_id
    from {{ ref('dim_person_gender') }}
    where gender = 'Female'
),

hirsutism as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs13") }})
),

pcos_excluded as (
    select bp.person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }} as bp
    inner join female_patients as f
        on bp.person_id = f.person_id
    inner join hirsutism as h
        on bp.person_id = h.person_id
    where bp.age < 40
)

select distinct b.person_id
from hf_base as b
inner join arm_med_spiro as s
    on b.person_id = s.person_id
inner join included as i
    on b.person_id = i.person_id
where b.person_id not in (select person_id from pcos_excluded)

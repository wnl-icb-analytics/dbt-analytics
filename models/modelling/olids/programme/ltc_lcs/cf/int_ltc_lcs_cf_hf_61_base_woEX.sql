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
-- ICB_CF_HF_61_woEX: R1-R6 SELECT immediately. Otherwise R7 requires a spironolactone
-- order strictly after three months ago, then R8 rejects only females aged < 40 with
-- hirsutism, acne or alopecia. Spironolactone is an independent inclusion route.
-- R1: HF-specific medications; R2: SGLT2/digoxin group; R6: digoxin. All use > -3 months.
-- R3: latest pro-BNP > 400, with no linked cardiology referral, HF-excluded or normal/
-- diastolic-dysfunction LV echo observation on or after the pro-BNP date.
-- R4: pro-BNP strictly within two years, latest > 2000, with no HF-excluded observation
-- on or after that date. Select the latest result before applying each value test.
-- R5: the latest record in each cardiomyopathy/HF-excluded and breathlessness/
-- HF-excluded pool must not be an HF-excluded observation.
-- One row per eligible person; HF_61 applies the final three-year exclusion separately.
--
-- Valueset notes:
--   vs1 friendly name COLLIDES (woEX Sacubitril/Valsartan id 299f9e3b... vs final-search
--     "HF excluded" id 9228a15e...). The macro matches every list sharing a friendly name,
--     so the woEX id is pinned here and the final-search id in int_ltc_lcs_cf_hf_61.
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
    from ({{ get_ltc_lcs_medication_orders_latest("299f9e3b-bbab-c65e-c19e-9c531f91755a, hf_case_finding_eligible_patients_vs2, hf_case_finding_eligible_patients_vs3") }})
    where order_date > dateadd(month, -3, current_date())
),

-- med_sglt2: Dapagliflozin / Digoxin group (vs4), 3m
arm_med_sglt2 as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs4") }})
    where order_date > dateadd(month, -3, current_date())
),

bnp_observations as (
    select observation_id, person_id, clinical_effective_date, result_value
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs5") }})
),

bnp_400_latest as (
    select *
    from bnp_observations
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

bnp_follow_up as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs6") }})
),

hf_excluded as (
    select observation_id, person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs7") }})
),

arm_bnp_400 as (
    select b.person_id
    from bnp_400_latest as b
    where b.result_value > 400
        and not exists (
            select 1
            from bnp_follow_up as f
            where f.person_id = b.person_id
                and f.clinical_effective_date >= b.clinical_effective_date
        )
),

bnp_2000_latest as (
    select *
    from bnp_observations
    where clinical_effective_date > dateadd(year, -2, current_date())
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

arm_bnp_2000 as (
    select b.person_id
    from bnp_2000_latest as b
    where b.result_value > 2000
        and not exists (
            select 1
            from hf_excluded as e
            where e.person_id = b.person_id
                and e.clinical_effective_date >= b.clinical_effective_date
        )
),

-- Rank the whole pool before excluding HF-excluded records. Ranking per value set
-- would allow an older symptom to survive a later HF-excluded record.
symptom_pool_a_latest as (
    select observation_id, person_id
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs8, hf_case_finding_eligible_patients_vs7") }})
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

symptom_pool_b_latest as (
    select observation_id, person_id
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs9, hf_case_finding_eligible_patients_vs7") }})
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

arm_symptom as (
    select a.person_id
    from symptom_pool_a_latest as a
    inner join symptom_pool_b_latest as b
        on a.person_id = b.person_id
    where not exists (
        select 1 from hf_excluded as e
        where e.observation_id = a.observation_id
    )
        and not exists (
            select 1 from hf_excluded as e
            where e.observation_id = b.observation_id
        )
),

-- med_digoxin: Digoxin (vs10), 3m
arm_med_digoxin as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs10") }})
    where order_date > dateadd(month, -3, current_date())
),

-- med_spiro: Spironolactone (vs11), 3m
arm_med_spiro as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("hf_case_finding_eligible_patients_vs11") }})
    where order_date > dateadd(month, -3, current_date())
),

-- R1-R6 select before the spironolactone route and its carve-out are evaluated.
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

-- R8 applies only after R7. The vs13 list includes hirsutism, acne and alopecia.
-- vs12 (Female) is an EMIS-internal attribute, resolved through dim_person_gender.
female_patients as (
    select distinct person_id
    from {{ ref('dim_person_gender') }}
    where gender = 'Female'
),

pcos_codes as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("hf_case_finding_eligible_patients_vs13") }})
),

pcos_excluded as (
    select bp.person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }} as bp
    inner join female_patients as f
        on bp.person_id = f.person_id
    inner join pcos_codes as h
        on bp.person_id = h.person_id
    where bp.age < 40
),

spiro_route as (
    select distinct s.person_id
    from arm_med_spiro as s
    where not exists (
        select 1 from pcos_excluded as e
        where e.person_id = s.person_id
    )
),

selected as (
    select person_id from included
    union
    select person_id from spiro_route
)

select b.person_id
from hf_base as b
inner join selected as s
    on b.person_id = s.person_id

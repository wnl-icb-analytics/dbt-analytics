{{ config(materialized='table') }}

-- LTC LCS HF case finding indicator HF_61: possible undiagnosed heart failure
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hf/icb-cf-hf-61.md
-- ICB_CF_HF_61 = ICB_CF_HF_61_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with a "Heart failure excluded" code in the last 3 years.
--   The friendly name hf_case_finding_eligible_patients_vs1 COLLIDES (woEX copy =
--   Sacubitril/Valsartan medication, id 299f9e3b...; final copy = "Heart failure
--   excluded", id 9228a15e..., hash 15a4071a). The final-search marker id is pinned
--   here so Rule 1 uses the correct single "Heart failure excluded" code.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_hf_61_base_woEX') }}
),

-- Rule 1: "Heart failure excluded" recorded in the last 3 years (pinned final-search id)
hf_excluded as (
    select person_id
    from ({{ get_ltc_lcs_observations("9228a15e-e6cb-1dd2-6074-0c306c800c1b") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from hf_excluded)

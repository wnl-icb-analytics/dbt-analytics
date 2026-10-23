{{ config(materialized='table') }}

-- CKD_61: one row per eligible, registered, living person aged at least 17.
-- The latest positive six-code eGFR and latest positive result on an earlier date
-- must both be < 60, with at least two positive eight-code observations.
-- Excludes screening strictly within three years and current CKD register membership.
-- Register membership uses the QOF CKD register (fct_person_ckd_register)
-- as a proxy for the EMIS LTC LCS CKD register.
-- Authority: EMIS R5 CKD case-finding reports; full result rules are in the helper.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_woEX') }}
),

-- Rule 1 (1D): CKD screening recorded in the last 3 years (1-code marker, pinned id)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("6e168908-5577-8967-f124-d109c66126e1") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
),

-- Rule 1 (1N): on the CKD register
ckd_register as (
    select person_id
    from {{ ref('fct_person_ckd_register') }}
    where is_on_register = true
)

select distinct person_id
from base_woex
where person_id not in (select person_id from ckd_screened)
    and person_id not in (select person_id from ckd_register)

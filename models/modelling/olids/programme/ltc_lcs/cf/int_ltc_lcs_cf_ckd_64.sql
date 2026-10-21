{{ config(materialized='table') }}

-- CKD_64: one row per eligible, registered, living person aged at least 17.
-- Includes the CKD_64 helper at-risk arms after CKD_61/62/63 helper exclusions.
-- Excludes screening strictly within three years and current CKD register membership.
-- Register membership uses the QOF CKD register (fct_person_ckd_register)
-- as a proxy for the EMIS LTC LCS CKD register.
-- Authority: EMIS R5 CKD case-finding reports; full result rules are in the helper.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_64_woEX') }}
),

-- Rule 1 (1D): CKD screening recorded in the last 3 years (1-code marker, pinned id)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("7b63b8f0-5846-9636-b5b0-7c2e2f177f30") }})
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

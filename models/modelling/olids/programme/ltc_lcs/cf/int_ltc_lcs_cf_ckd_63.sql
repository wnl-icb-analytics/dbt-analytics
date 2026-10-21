{{ config(materialized='table') }}

-- CKD_63: one row per eligible, registered, living person aged at least 17.
-- The latest positive exact-code UACR must be > 70. Excludes CKD_61/62 helpers.
-- Excludes screening strictly within three years and current CKD register membership.
-- Authority: EMIS R5 CKD case-finding reports; full result rules are in the helper.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_63_woEX') }}
),

-- Rule 1 (1D): CKD screening recorded in the last 3 years (1-code marker)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("21ca7a93-5261-c6d6-0e7d-29f497aeb753") }})
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

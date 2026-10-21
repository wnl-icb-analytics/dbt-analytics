{{ config(materialized='table') }}

-- CKD_62: one row per eligible, registered, living person aged at least 17.
-- The latest positive exact-code UACR and latest positive result on an earlier date
-- must both be > 4, with at least two positive observations. Excludes the CKD_61 helper.
-- Excludes screening strictly within three years and current CKD register membership.
-- Authority: EMIS R5 CKD case-finding reports; full result rules are in the helper.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_62_woEX') }}
),

-- Rule 1: CKD screening recorded in the last 3 years (1-code marker)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("1daf7ba0-0a74-840c-3af3-83affee70a7f") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
),

-- on the CKD register
ckd_register as (
    select person_id
    from {{ ref('fct_person_ckd_register') }}
    where is_on_register = true
)

select distinct person_id
from base_woex
where person_id not in (select person_id from ckd_screened)
    and person_id not in (select person_id from ckd_register)

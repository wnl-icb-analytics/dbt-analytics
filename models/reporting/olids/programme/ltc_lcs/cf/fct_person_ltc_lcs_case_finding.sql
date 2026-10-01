{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- LTC LCS case-finding cohort membership, one row per person currently in any
-- case-finding indicator. Lean person-level fact used as the snapshot source for
-- monthly cohort tracking; excludes the in_any_case_finding=false base population
-- carried by dim_ltc_lcs_cf_summary.

select
    person_id,
    in_af_61,
    in_af_62,
    in_hf_61,
    in_ckd_61,
    in_ckd_62,
    in_ckd_63,
    in_ckd_64,
    in_cvd_61,
    in_cvd_62,
    in_cvd_63,
    in_cvd_64,
    in_cvd_65,
    in_cvd_66,
    in_dm_61,
    in_dm_62,
    in_dm_63,
    in_dm_64,
    in_dm_65,
    in_dm_66,
    in_htn_61,
    in_htn_62,
    in_htn_63,
    in_htn_65,
    in_htn_66,
    in_cyp_ast_61,
    in_any_case_finding,
    case_finding_count,
    current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('dim_ltc_lcs_cf_summary') }}
where in_any_case_finding = true

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Reporting-layer eFI2 (electronic Frailty Index 2) score + frailty category per
person.

Joins the in-house computed score (int_efi2_scores) to dim_person — only persons
with at least one linked patient record appear here.

No opt-out filter is applied at this layer. For secondary use, consumers should
INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED
ON person_id per the project convention.

Native replacement for the retired AIC import stg_aic_int_efi2_scores
(AIC_DEV.INT_EFI2_SCORES), which was removed with the rest of the raw_aic pipeline.
*/

select
    s.person_id,
    s.end_date,
    s.efi_score,
    s.category,
    s.age_at_end,
    s.gender,
    s.date_of_death
from {{ ref('int_efi2_scores') }} s
inner join {{ ref('dim_person') }} p
    on s.person_id = p.person_id

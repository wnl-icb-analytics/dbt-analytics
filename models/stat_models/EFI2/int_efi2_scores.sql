{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}

/*
eFI2 (electronic Frailty Index 2) score and frailty category per person.

Native OLIDS-built equivalent of the AI Centre's INT_EFI2_SCORES, ported from
lds-pipelines (branch 190-chore-diff-sel-ncl). Deficit codelist + weights are the
published inputs now sourced from DATA_LAKE__NCL.COMMON (stg_common_efi2_*);
the scoring logic mirrors lds.

The frailty category cut points match lds (ROBUST < 0.0857, MILD to < 0.1624,
MODERATE to <= 0.2391, SEVERE above), but are expressed as ordered thresholds
rather than lds's paired BETWEEN ranges. lds's ranges left the interval
(0.1623, 0.1624) uncovered -> NULL category; the ordered form closes that gap
(the sliver resolves to MILD) without changing any other band assignment.
*/

with scored as (
    select distinct
        pl.person_id,
        pl.end_date,
        nvl(score.efi, 0) as efi_score,
        obs.age_at_end,
        obs.gender,
        pl.date_of_death
    from {{ ref("int_efi2_patient_list") }} pl
    left join
        {{ ref("int_efi2_cohort_snomed_codes") }} obs
        on pl.person_id = obs.person_id
        and pl.end_date = obs.end_date
    left join
        {{ ref("int_efi2_chronology") }} score
        on pl.person_id = score.person_id
        and pl.end_date = score.end_date
)

select
    person_id,
    end_date,
    efi_score,
    {{ efi2_category('efi_score') }} as category,
    age_at_end,
    gender,
    date_of_death
from scored

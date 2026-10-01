{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_efi2 for SCD2 snapshotting.

fct_person_efi2 carries CURRENT_DATE()-derived columns -- end_date (the single scoring-run
date, = current_date()) and age_at_end (age at that date) -- which advance on every build, so
snapshotting the full model with check_cols: all would version every person on every run. This
view exposes only the stable frailty measures (the eFI2 score and its category band), so the
snapshot versions only on a genuine change (a new score / band). Kept separate from Rockwood to
mirror how cltcs_cohort_data reads frailty.

Downstream: snapshots/fct_person_efi2_snapshot.yml
*/

select
    person_id
    , efi_score
    , category
from {{ ref('fct_person_efi2') }}

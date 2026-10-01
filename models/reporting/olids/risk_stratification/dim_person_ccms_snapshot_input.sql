{{ config(materialized='view') }}

/*
Thin snapshot-input projection of dim_person_ccms for SCD2 snapshotting.

dim_person_ccms carries `last_updated` and `ccms_score_id`, which change on every
run (the surrogate key is built over last_updated), so snapshotting the full model
with check_cols: all would version every person on every build. This view exposes
only the stable, meaningful score so the snapshot tracks genuine changes.

Downstream: snapshots/dim_person_ccms_snapshot.yml
*/

select
    person_id
    , cambridge_comorbidity_score
from {{ ref('dim_person_ccms') }}

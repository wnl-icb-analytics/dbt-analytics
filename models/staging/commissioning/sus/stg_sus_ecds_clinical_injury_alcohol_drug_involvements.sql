{{
    config(materialized = 'view')
}}

select
    primarykey_id
    , alcohol_drug_involvements_id
    , rownumber_id
    , code
from {{ ref('raw_sus_ecds_clinical_injury_alcohol_drug_involvements') }}

{{
    config(materialized = 'view')
}}

select primarykey_id
    ,episodes_id
    ,opcs_id 
    ,rownumber_id
    ,code
from {{ ref('raw_sus_apc_spell_episodes_clinical_coding_procedure_opcs') }}
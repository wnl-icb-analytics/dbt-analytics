{{
    config(materialized = 'view')
}}

select primarykey_id
    , {{ dbt_utils.generate_surrogate_key(['primarykey_id', 'episodes_id', 'opcs_id']) }} as procedure_id
    , try_to_date(date::varchar) as procedure_date
    ,episodes_id
    ,opcs_id 
    ,rownumber_id
    ,code
from {{ ref('raw_sus_apc_spell_episodes_clinical_coding_procedure_opcs') }}
-- Identical source replays do not represent another procedure. Conflicting slots remain visible.
qualify row_number() over (
    partition by primarykey_id, episodes_id, opcs_id, code, try_to_date(date::varchar)
    order by rownumber_id
) = 1

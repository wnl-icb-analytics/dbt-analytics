-- The APC long-form clinical models retain every non-null source code. Summing
-- observation_count restores source-record grain after repeated codes are
-- collapsed within each spell.
with expected as (
    select 'diagnosis' as observation_type, count(*) as source_records
    from {{ ref('stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd') }}
    where code is not null

    union all

    select 'procedure', count(*)
    from {{ ref('stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs') }}
    where code is not null
),

actual as (
    select 'diagnosis' as observation_type, sum(observation_count) as model_records
    from {{ ref('int_sus_apc_diagnosis') }}

    union all

    select 'procedure', sum(observation_count)
    from {{ ref('int_sus_apc_procedure') }}
)

select
    coalesce(e.observation_type, a.observation_type) as observation_type
    , e.source_records
    , a.model_records
from expected as e
full outer join actual as a using (observation_type)
where not equal_null(e.source_records, a.model_records)

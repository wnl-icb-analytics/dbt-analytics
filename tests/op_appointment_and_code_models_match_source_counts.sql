-- The outpatient models retain every staged appointment and every usable
-- clinical code. Summing observation_count restores source-record grain after
-- repeated codes are collapsed within each appointment. Aggregate categories
-- keep any failing-test output free of record-level data.
with expected as (
    select 'appointment' as record_type, count(*) as source_records
    from {{ ref('stg_sus_op_appointment') }}

    union all

    select 'diagnosis', count(*)
    from {{ ref('stg_sus_op_appointment_clinical_coding_diagnosis_icd') }}
    where code is not null

    union all

    select 'procedure', count(*)
    from {{ ref('stg_sus_op_appointment_clinical_coding_procedure_opcs') }}
    where code is not null

    union all

    select 'hrg', count(*)
    from (
        select primarykey_id, unbundled_hrg_id as problem_order, code
        from {{ ref('stg_sus_op_appointment_commissioning_grouping_unbundled_hrg') }}
        where code is not null

        union

        select
            primarykey_id,
            0::number as problem_order,
            appointment_commissioning_grouping_core_hrg as code
        from {{ ref('stg_sus_op_appointment') }}
        where appointment_commissioning_grouping_core_hrg is not null
    )
),

actual as (
    select 'appointment' as record_type, count(*) as model_records
    from {{ ref('int_sus_op_appointment') }}

    union all

    select 'diagnosis', sum(observation_count)
    from {{ ref('int_sus_op_diagnosis') }}

    union all

    select 'procedure', sum(observation_count)
    from {{ ref('int_sus_op_procedure') }}

    union all

    select 'hrg', sum(observation_count)
    from {{ ref('int_sus_op_procedure_hrg') }}
)

select
    coalesce(e.record_type, a.record_type) as record_type,
    e.source_records,
    a.model_records
from expected as e
full outer join actual as a using (record_type)
where not equal_null(e.source_records, a.model_records)

{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['polypharmacy']
    )
}}

-- Latest eligible repeat issue per person and product, retained while its supply is current.
with repeat_prescription_codes as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where cluster_id = 'REPEAT_PRESCRIPTION'
),
repeat_orders as (
    select
        mo.person_id,
        mo.id as medication_order_id,
        mo.clinical_effective_date as order_date,
        mo.date_recorded,
        mo.duration_days as order_duration_days,
        mo.mapped_concept_code,
        {{ medication_supply_columns('mo.duration_days', 'mo.clinical_effective_date', 'ms.cancellation_date') }}
    from {{ ref('stg_olids_medication_order') }} mo
    inner join {{ ref('stg_olids_medication_statement') }} ms
        on mo.medication_statement_id = ms.id
    inner join repeat_prescription_codes rpc on ms.authorisation_type_code = rpc.code
    where mo.clinical_effective_date is not null
        and mo.clinical_effective_date <= current_date()
        and mo.mapped_concept_code is not null
),
latest_orders as (
    select *
    from repeat_orders
    qualify row_number() over (
        partition by person_id, mapped_concept_code
        order by order_date desc, date_recorded desc nulls last, medication_order_id desc
    ) = 1
)
select
    person_id,
    mapped_concept_code,
    order_date as latest_order_date,
    order_duration_days as latest_duration,
    medication_order_id,
    effective_duration_days,
    is_duration_imputed,
    supply_end_date
from latest_orders
-- Filtering after ranking prevents an older issue replacing a stopped latest issue.
where supply_end_date >= current_date()

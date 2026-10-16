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
        mo.mapped_concept_code
    from {{ ref('stg_olids_medication_order') }} mo
    inner join {{ ref('stg_olids_medication_statement') }} ms
        on mo.medication_statement_id = ms.id
    inner join repeat_prescription_codes rpc on ms.authorisation_type_code = rpc.code
    where mo.clinical_effective_date is not null
        and mo.clinical_effective_date <= current_date()
        and mo.mapped_concept_code is not null
),
latest_dates as (
    select person_id, mapped_concept_code, max(order_date) as latest_order_date
    from repeat_orders
    group by person_id, mapped_concept_code
),
latest_orders as (
    select ro.*
    from repeat_orders ro
    inner join latest_dates ld
        on ro.person_id = ld.person_id
        and ro.mapped_concept_code = ld.mapped_concept_code
        and ro.order_date = ld.latest_order_date
    qualify row_number() over (
        partition by ro.person_id, ro.mapped_concept_code
        order by ro.date_recorded desc nulls last, ro.medication_order_id desc
    ) = 1
)
select
    person_id,
    mapped_concept_code,
    order_date as latest_order_date,
    order_duration_days as latest_duration,
    medication_order_id
from latest_orders
where dateadd(day, coalesce(order_duration_days, 28), order_date) >= current_date()

{{ config(tags=['reconciliation']) }}
-- Dated person-linked PPI orders must survive without a commissioning key.
select mo.id as medication_order_id
from {{ ref('stg_olids_medication_order') }} as mo
where mo.bnf_chapter = '01'
    and mo.bnf_code like '0103050%'
    and mo.clinical_effective_date is not null
    and mo.person_id is not null
    and not exists (
        select 1 from {{ ref('int_ppi_medications_all') }} as m
        where m.medication_order_id = mo.id
    )

{% macro get_ltc_lcs_insulin_medication_orders() %}
-- The national dm+d list retains older products. The SNOMED insulin-product hierarchy
-- adds current products, including Lyumjev and Trurapi, without insulin devices.
with insulin_codes as (
    select code
    from {{ ref('stg_reference_combined_codesets') }}
    where source = 'OPENCODELISTS' and cluster_id = 'OPENSAFELY/INSULIN_MEDICATION'
    union
    select sub_type_id::varchar
    from {{ ref('stg_nhsd_snomed_sct_transitive_closure') }}
    where super_type_id = 39487003 -- Product containing insulin
    union
    select '39487003'
)
select mo.person_id, mo.clinical_effective_date as order_date
from {{ ref('stg_olids_medication_order') }} as mo
inner join insulin_codes as insulin on mo.mapped_concept_code = insulin.code
where mo.clinical_effective_date is not null
{% endmacro %}

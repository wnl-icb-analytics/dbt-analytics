{% macro ltc_lcs_refset_members() %}

with pcd_members as (
    select pcd_refset_id::varchar as refset_id, snomed_code
    from {{ ref('stg_reference_pcd_refset_latest') }}
    where pcd_refset_id is not null
)
select distinct refset_id, snomed_code
from pcd_members
union all
-- PCD membership comes from UKHFD; NHSD supplies other active simple refsets.
select distinct member.ref_set_id::varchar as refset_id,
    member.referenced_component_id::varchar as snomed_code
from {{ ref('stg_nhsd_snomed_sct_refset_simple') }} as member
where member.active = 1
    and not exists (
        select 1 from pcd_members as pcd
        where pcd.refset_id = member.ref_set_id::varchar
    )

{% endmacro %}

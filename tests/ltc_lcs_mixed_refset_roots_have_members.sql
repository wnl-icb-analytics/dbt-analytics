-- Mixed sets need resolved refsets because their mapped expansion is restricted.
-- Public-only sets keep the supplied expansion without resolving roots here.
with refset_members as (
    {{ ltc_lcs_refset_members() }}
)
select distinct root.valueset_id, root.original_code
from {{ ref('stg_reference_ltc_lcs_original_codes') }} as root
where root.is_refset
    and exists (
        select 1
        from {{ ref('stg_reference_ltc_lcs_original_codes') }} as local_root
        where local_root.valueset_id = root.valueset_id
            and regexp_like(local_root.original_code, '.*1000006[0-9]{3}$')
    )
    and not exists (
        select 1 from refset_members as member
        where member.refset_id = root.original_code
    )

with products as (
    select vpid as product_code, 'VMP' as product_type, vpid from {{ ref('stg_ukhfd_dmd_vmp') }}
    union all
    select apid, 'AMP', vpid from {{ ref('stg_ukhfd_dmd_amp') }}
),
ingredients as (
    select vpid, count(*) as ingredient_count
    from {{ ref('stg_ukhfd_dmd_vmp_ingredient') }}
    group by vpid
)
select
    p.product_code, p.product_type, p.vpid,
    v.vmp_name, v.vtm_id, t.vtm_name,
    f.form_code, fd.form_description, i.ingredient_count
from products p
left join {{ ref('stg_ukhfd_dmd_vmp') }} v on v.vpid = p.vpid
left join {{ ref('stg_ukhfd_dmd_vtm') }} t on t.vtm_id = v.vtm_id
left join {{ ref('stg_ukhfd_dmd_vmp_drug_form') }} f on f.vpid = p.vpid
left join {{ ref('stg_ukhfd_dmd_lookup_form') }} fd on fd.form_code = f.form_code
left join ingredients i on i.vpid = p.vpid

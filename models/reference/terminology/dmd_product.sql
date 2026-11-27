with products as (
    select vpid as product_code, 'VMP' as product_type, vpid from {{ ref('stg_ukhfd_dmd_vmp') }}
    union all
    select apid, 'AMP', vpid from {{ ref('stg_ukhfd_dmd_amp') }}
),
ingredient_rows as (
    -- Current ingredient rows when the product has any; otherwise the retained rows of a removed product.
    -- Counting every retained row would add superseded ingredient records to current products.
    select vpid, strength_numerator_value, strength_numerator_unit_code,
        strength_denominator_value, strength_denominator_unit_code
    from {{ ref('stg_ukhfd_dmd_vmp_ingredient') }}
    qualify coalesce(is_in_latest_source, false)
        = max(coalesce(is_in_latest_source, false)) over (partition by vpid)
),
ingredients as (
    select vpid, count(*) as ingredient_count,
        max(strength_numerator_value) as strength_numerator_value,
        max(strength_numerator_unit_code) as strength_numerator_unit_code,
        max(strength_denominator_value) as strength_denominator_value,
        max(strength_denominator_unit_code) as strength_denominator_unit_code
    from ingredient_rows
    group by vpid
)
select
    p.product_code, p.product_type, p.vpid,
    v.vmp_name, v.vtm_id, t.vtm_name,
    f.form_code, fd.form_description, i.ingredient_count,
    -- Liquid strengths are concentrations. Recover the amount in one vial/syringe.
    case when i.ingredient_count = 1 then
        i.strength_numerator_value::NUMBER(28, 6)
        * case i.strength_numerator_unit_code
            when '258684004' then 1       -- mg
            when '258685003' then 0.001   -- microgram
            when '258682000' then 1000    -- g
          end
        * case
            when i.strength_denominator_value is null
                and i.strength_denominator_unit_code is null then 1
            when i.strength_denominator_value > 0
                and v.unit_dose_form_size > 0
                and i.strength_denominator_unit_code = v.unit_dose_form_size_unit_code
                -- UKHFD floats can hold 1.7 ml as 1.7000000477. dm+d sizes use decimal precision.
                then v.unit_dose_form_size::NUMBER(28, 6) / i.strength_denominator_value::NUMBER(28, 6)
          end
    end as unit_dose_strength_mg
from products p
left join {{ ref('stg_ukhfd_dmd_vmp') }} v on v.vpid = p.vpid
left join {{ ref('stg_ukhfd_dmd_vtm') }} t on t.vtm_id = v.vtm_id
left join {{ ref('stg_ukhfd_dmd_vmp_drug_form') }} f on f.vpid = p.vpid
left join {{ ref('stg_ukhfd_dmd_lookup_form') }} fd on fd.form_code = f.form_code
left join ingredients i on i.vpid = p.vpid

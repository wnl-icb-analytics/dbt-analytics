with fiscal_years as (
    select
        fiscal_year_start
        , gdp_deflator
        , case when fiscal_year_start = min(fiscal_year_start) over ()
            then '1900-01-01'::date
            else date_from_parts(fiscal_year_start, 4, 1)
        end as fy_range_start
        , case when fiscal_year_start = max(fiscal_year_start) over ()
            then '2099-12-31'::date
            else date_from_parts(fiscal_year_start + 1, 3, 31)
        end as fy_range_end
    from {{ ref('uk_cost_indices') }}
)

, price_base_deflator as (
    select max(gdp_deflator) as base_gdp_deflator
    from {{ ref('uk_cost_indices') }}
    where fiscal_year_start = 2026
)

, priced as (
    select
        c.*
        , coalesce(price.unit_price_2627_gbp, age_default.unit_price_2627_gbp) as unit_price_2627_gbp
        , case
            when price.unit_price_2627_gbp is not null then 'exact'
            when age_default.unit_price_2627_gbp is not null then 'age_default'
        end as price_source
    from {{ ref('int_csds_contact_currency') }} as c
    left join {{ ref('int_nhse_currency_price_resolution') }} as price
        on c.currency_code = price.currency_code
    -- every seed-mapped community code is priced; the age default is a
    -- guard for codes absent from the schedule entirely
    left join {{ ref('int_nhse_currency_price_resolution') }} as age_default
        on iff(c.age_category = 'CYP', 'CCO99Z', 'CAO99Z') = age_default.currency_code
)

select
    p.unique_service_request_identifier
    , p.unique_care_contact_identifier
    , p.sk_patient_id
    , p.person_id
    , p.org_id_prov
    , p.care_contact_date
    , p.dm_icb_commissioner
    , p.dm_sub_icb_commissioner
    , p.commissioner_icb_code
    , p.practice_code
    , p.practice_attribution
    , p.practice_name
    , p.practice_metadata_source
    , p.pcn_code
    , p.pcn_name
    , p.practice_registered_borough
    , p.lsoa21_residence
    , p.residence_borough
    , p.sub_icb_of_residence
    , fy.fiscal_year_start
    , p.attendance_status
    , p.is_costed_attendance
    , p.age_category
    , p.currency_code
    , p.currency_source
    , p.unit_price_2627_gbp
    , p.price_source
    , coalesce(mff.mff_factor, 1.0) as mff_factor
    , iff(
        p.is_costed_attendance
        , p.unit_price_2627_gbp * coalesce(mff.mff_factor, 1.0) * fy.gdp_deflator / pb.base_gdp_deflator
        , 0
    ) as proxy_cost
from priced as p
cross join price_base_deflator as pb
left join fiscal_years as fy
    on p.care_contact_date between fy.fy_range_start and fy.fy_range_end
left join {{ ref('provider_market_forces_factor_2026_27') }} as mff
    on p.org_id_prov = mff.provider_code

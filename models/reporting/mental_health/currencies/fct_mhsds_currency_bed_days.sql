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

, activity as (
    select
        c.*
        -- MAZ99 is the published grouper code for cross-cutting crisis,
        -- including inpatient spells. It has only a contact price, so use
        -- the unclassified inpatient price for the recorded bed setting.
        , case
            when c.currency_group = 'MAZ'
                then 'MBU98' || coalesce(c.setting_code, 'Z')
            else c.currency_code
        end as pricing_currency_code
        -- open spells accrue cost only to their last submission evidence:
        -- the active feed runs ~6 weeks behind, so accruing to today would
        -- cost unevidenced nights
        , greatest(
            coalesce(c.end_date, c.last_submission_period_end, current_date)
            , dateadd(day, 1, c.start_date_hosp_prov_spell)
        ) as activity_end_date
    from {{ ref('int_mhsds_spell_currency') }} as c
)

, priced as (
    select
        a.*
        , price.unit_price_2627_gbp
        , price.price_source
    from activity as a
    left join {{ ref('int_nhse_currency_price_resolution') }} as price
        on a.pricing_currency_code = price.currency_code
)

select
    p.uniq_hosp_prov_spell_num
    , fy.fiscal_year_start
    , p.uniq_serv_req_id
    , p.sk_patient_id
    , p.person_id
    , p.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , p.dm_icb_commissioner as source_derived_icb_commissioner_code
    , derived_icb.organisation_name as source_derived_icb_commissioner_name
    , p.commissioner_icb_code
    , currency_commissioner.organisation_name as commissioner_icb_name
    , coalesce(currency_commissioner.is_wnl_commissioner, false) as is_wnl_commissioner
    , p.currency_group
    , p.currency_code
    , p.pricing_currency_code
    , p.start_date_hosp_prov_spell as hospital_provider_spell_start_date
    , p.end_date as hospital_provider_spell_end_date
    -- date window this row's bed days cover; to-date exclusive
    , greatest(p.start_date_hosp_prov_spell, fy.fy_range_start) as bed_days_from_date
    , least(p.activity_end_date, dateadd(day, 1, fy.fy_range_end)) as bed_days_to_date
    , datediff(day
        , greatest(p.start_date_hosp_prov_spell, fy.fy_range_start)
        , least(p.activity_end_date, dateadd(day, 1, fy.fy_range_end))
    ) as bed_days
    , p.unit_price_2627_gbp
    , case
        when p.currency_group = 'MAZ'
            and p.setting_code in ('A', 'B', 'C')
            and p.price_source = 'exact'
            then 'unclassified_setting'
        when p.currency_group = 'MAZ' then 'unclassified_family'
        else p.price_source
    end as price_source
    , coalesce(mff.mff_factor, 1.0) as mff_factor
    , fy.gdp_deflator / pb.base_gdp_deflator as gdp_deflator_ratio_applied
    , datediff(day
        , greatest(p.start_date_hosp_prov_spell, fy.fy_range_start)
        , least(p.activity_end_date, dateadd(day, 1, fy.fy_range_end))
    ) * p.unit_price_2627_gbp
        * coalesce(mff.mff_factor, 1.0)
        * fy.gdp_deflator / pb.base_gdp_deflator as proxy_cost
    , p.end_date_source as hospital_provider_spell_end_date_source
    , p.is_cyp
    , p.winning_tier
from priced as p
cross join price_base_deflator as pb
inner join fiscal_years as fy
    on fy.fy_range_start <= p.activity_end_date
    and fy.fy_range_end >= p.start_date_hosp_prov_spell
    and datediff(day
        , greatest(p.start_date_hosp_prov_spell, fy.fy_range_start)
        , least(p.activity_end_date, dateadd(day, 1, fy.fy_range_end))
    ) > 0
left join {{ ref('provider_market_forces_factor_2026_27') }} as mff
    on p.org_id_prov = mff.provider_code
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(p.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as derived_icb
    on upper(p.dm_icb_commissioner) = upper(derived_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as currency_commissioner
    on upper(p.commissioner_icb_code) = upper(currency_commissioner.organisation_code)

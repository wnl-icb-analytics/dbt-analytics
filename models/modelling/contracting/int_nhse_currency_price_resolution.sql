/*
Resolved 2026/27 unit price for every currency code a classifier can emit.

Grain: one row per currency_code. Population: every code in the NHSE price
schedule plus every code constructible from the MH population groups x
families (96/97/98/99) x settings (A-D/Z) - classifiers can emit codes the
schedule does not publish (e.g. MAZ98A).

Resolution chain, first non-null price wins (out-of-scope schedule rows
carry NULL prices and fall through):
  1. exact code
  2. same population group and family, setting Z
  3. MBU (unclassified) for the family and setting - 99-family codes fall
     back to MBU97 crisis prices, though MAZ99Z/MCS99Z are priced so this
     only fires for 96/97/98
  4. MBU for the family, setting Z
Non-MH codes (community CA/CC, Talking Therapies, ADHD/ASD) resolve at
step 1 only.

Prices are the 26/27 base; consumers rebase with the GDP deflator and
apply the provider MFF.
*/

with schedule as (
    select
        currency_code
        , nullif(population_group, '') as population_group
        , nullif(currency_family, '') as currency_family
        , nullif(setting_code, '') as setting_code
        , unit
        , price_gbp
    from {{ ref('nhse_currency_prices_2627') }}
)

, constructible as (
    select
        pg.currency_group || f.family || s.setting_code as currency_code
        , pg.currency_group as population_group
        , f.family as currency_family
        , s.setting_code
    from {{ ref('nhse_mh_currency_population_groups_2627') }} as pg
    cross join (
        select column1 as family
        from values ('96'), ('97'), ('98'), ('99')
    ) as f
    cross join (
        select column1 as setting_code
        from values ('A'), ('B'), ('C'), ('D'), ('Z')
    ) as s
)

, universe as (
    select currency_code, population_group, currency_family, setting_code
    from schedule
    union
    select currency_code, population_group, currency_family, setting_code
    from constructible
)

select
    u.currency_code
    , u.population_group
    , u.currency_family
    , u.setting_code
    , coalesce(exact.unit, popz.unit, mbu_setting.unit, mbuz.unit) as unit
    , coalesce(
        exact.price_gbp
        , popz.price_gbp
        , mbu_setting.price_gbp
        , mbuz.price_gbp
    ) as unit_price_2627_gbp
    , case
        when exact.price_gbp is not null then 'exact'
        when popz.price_gbp is not null then 'population_family'
        when mbu_setting.price_gbp is not null then 'unclassified_setting'
        when mbuz.price_gbp is not null then 'unclassified_family'
    end as price_source
from universe as u
left join schedule as exact
    on u.currency_code = exact.currency_code
left join schedule as popz
    on u.population_group = popz.population_group
    and u.currency_family = popz.currency_family
    and popz.setting_code = 'Z'
left join schedule as mbu_setting
    on mbu_setting.population_group = 'MBU'
    and mbu_setting.currency_family = iff(u.currency_family = '99', '97', u.currency_family)
    and mbu_setting.setting_code = u.setting_code
left join schedule as mbuz
    on mbuz.population_group = 'MBU'
    and mbuz.currency_family = iff(u.currency_family = '99', '97', u.currency_family)
    and mbuz.setting_code = 'Z'

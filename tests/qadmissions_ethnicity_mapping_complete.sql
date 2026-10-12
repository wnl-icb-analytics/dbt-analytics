-- Asserts the qadmissions_eth2016_to_ethrisk9 seed contains every
-- distinct `ethnicity_subcategory` value present in dim_person_ethnicity.
-- If a new subcategory appears in the population (e.g. a future ETH2016
-- cluster, or a new "no record" variant) it would silently be COALESCEd
-- to ethrisk = 1 in qadmissions_ethrisk - hiding data drift.
--
-- The test fails when the SELECT returns rows. It returns one row per
-- subcategory value that is in the live data but not in the seed.

with population as (
    select distinct ethnicity_subcategory
    from {{ ref('dim_person_ethnicity') }}
    where ethnicity_subcategory is not null
),

mapping as (
    select distinct ethnicity_subcategory
    from {{ ref('qadmissions_eth2016_to_ethrisk9') }}
)

select p.ethnicity_subcategory
from population p
left join mapping m
    on p.ethnicity_subcategory = m.ethnicity_subcategory
where m.ethnicity_subcategory is null

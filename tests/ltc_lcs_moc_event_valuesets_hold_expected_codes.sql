-- MoC event models label events by value set ID, so each set must hold exactly the
-- EMIS codes it is mapped to. Returns expected codes that are missing and unexpected
-- codes that are present.
with expected as (
    select column1 as valueset_id, column2 as original_code
    from values
        -- C&T, agreed, review, declined, offered (EMIS-local) plus care plan sharing
        ('06a67476-a82f-8c9e-d328-c0c7c5aea698', '552991000006110'),
        ('06a67476-a82f-8c9e-d328-c0c7c5aea698', '13072931000006119'),
        ('06a67476-a82f-8c9e-d328-c0c7c5aea698', '13072951000006114'),
        ('06a67476-a82f-8c9e-d328-c0c7c5aea698', '13489791000006113'),
        ('06a67476-a82f-8c9e-d328-c0c7c5aea698', '13489801000006114'),
        ('06a67476-a82f-8c9e-d328-c0c7c5aea698', '2230811000000113'),
        -- mdt_review_completed_vs1: annual review
        ('50bf74e8-ae15-b5f1-0e25-f6155f398c47', '2288591000000113'),
        -- careplan_sharing_completed_vs1: LTC summary sent to patient
        ('1e4b1d73-eaed-7e8d-da5e-0a77ca874c17', '2230811000000113'),
        -- CYP high risk annual review completed
        ('b7780aad-7443-896e-ebf6-1e3c3800226d', '1488421017'),
        -- CYP high risk desk top review completed
        ('fd4dd2f3-c33a-e7d2-3556-448b9e23662b', '2380081000000111')
),

actual as (
    select valueset_id, original_code
    from {{ ref('stg_reference_ltc_lcs_original_codes') }}
    where valueset_id in (select valueset_id from expected)
)

select coalesce(e.valueset_id, a.valueset_id) as valueset_id,
    coalesce(e.original_code, a.original_code) as original_code,
    case when a.original_code is null then 'missing' else 'unexpected' end as issue
from expected e
full outer join actual a
    on e.valueset_id = a.valueset_id
    and e.original_code = a.original_code
where e.original_code is null or a.original_code is null

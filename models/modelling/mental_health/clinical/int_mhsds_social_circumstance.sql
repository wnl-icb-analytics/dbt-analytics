-- Providers restate circumstances each month; one key per provider, person, code and recorded date.
with keyed as (
    select
        s.*
        , coalesce(s.soc_per_circumstance_rec_timestamp::date, s.soc_per_circumstance_rec_date) as recorded_date
        , {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.person_id', 's.soc_per_circumstance', 'recorded_date',
            "iff(s.org_id_prov is null or s.person_id is null or s.soc_per_circumstance is null or recorded_date is null, s.mhs011_uniq_id, null)"]) }} as source_record_id
    from {{ ref('stg_mhsds_social_circumstance') }} as s
)

select
    *
    , min(reporting_period_end_date) over (partition by source_record_id) as first_reported_period_end_date
    , max(reporting_period_end_date) over (partition by source_record_id) as last_reported_period_end_date
    , count(*) over (partition by source_record_id) as accepted_source_record_count
    , count(distinct reporting_period_end_date) over (partition by source_record_id) as reported_period_count
from keyed
qualify row_number() over (
    partition by source_record_id order by reporting_period_end_date desc nulls last,
        effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs011_uniq_id desc
) = 1

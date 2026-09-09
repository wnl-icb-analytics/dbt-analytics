-- Expanded observations must retain the event IDs previously read from each source.
select f.araf_referral_observation_id
from {{ ref('int_valproate_araf_referral_events') }} as f
left join {{ ref('stg_olids_observation') }} as o
    on f.araf_referral_observation_id = o.id
where o.id is null
    or f.source_entity is distinct from o.source_entity
    or f.source_record_id is distinct from o.source_record_id
    or f.araf_referral_id is distinct from case
        when o.source_entity = 'referral_request' then o.source_record_id
        else o.id
    end

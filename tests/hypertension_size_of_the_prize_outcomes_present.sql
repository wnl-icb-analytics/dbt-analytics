-- Fails when an outcome the hypertension_size_of_the_prize macro reads is missing from the
-- seed. The macro's scalar subqueries would then return null without any other test failing.
--
-- Returns one row per missing outcome.
select required.outcome
from (
    select 'heart_attack' as outcome
    union all select 'stroke'
    union all select 'death'
) as required
left join {{ ref('hypertension_size_of_the_prize') }} as rates
    on required.outcome = rates.outcome
where rates.outcome is null

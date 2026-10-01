-- Fails when the SLAM cost spine builds empty. The window anchor in
-- int_cost_index_slam_activity_monthly requires a month with >100k lines;
-- if none clears the threshold (feed truncation, upstream filter change)
-- every downstream fact builds empty and all schema tests pass vacuously.
--
-- Returns one row when the spine has no rows.
select row_count as spine_row_count
from (
    select count(*) as row_count
    from {{ ref('int_cost_index_slam_activity_monthly') }}
)
where row_count = 0

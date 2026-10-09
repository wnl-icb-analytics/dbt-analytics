-- Governed POD group mapping (POD Group Manager app). One row per
-- (pod_code, local_pod_code, local_pod_description) combination -> one of the
-- 18 curated POD groups. Key fields upper-cased/trimmed for joining to SLAM
-- feeds; empty strings collapsed to NULL (join with equal_null semantics --
-- many combinations have no local code/description, and ~13 lines have no POD
-- code at all, keyed on local code only).
-- The app stores raw-cased variants as distinct keys; after normalisation a
-- handful collapse to the same key, so dedupe to the most recently curated row.
select
    nullif(upper(trim(point_of_delivery_code)), '')              as pod_code,
    nullif(upper(trim(local_point_of_delivery_code)), '')        as local_pod_code,
    nullif(upper(trim(local_point_of_delivery_description)), '') as local_pod_description,
    pod_group_overview_master                                    as pod_group,
    notes,
    updated_at
from {{ ref('raw_pod_group_manager_pod_group_mapping') }}
qualify row_number() over (
    partition by pod_code, local_pod_code, local_pod_description
    order by updated_at desc nulls last, created_at desc nulls last
) = 1

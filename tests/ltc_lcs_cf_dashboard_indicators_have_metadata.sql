select
    indicator_id,
    count(*) as affected_row_count
from {{ ref('ltc_lcs_cf_dashboard_base') }}
where indicator_name is null
    or indicator_description is null
    or indicator_category is null
group by indicator_id

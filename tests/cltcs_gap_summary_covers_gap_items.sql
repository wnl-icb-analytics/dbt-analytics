-- Every item and domain group in cltcs_gap_items must have a column in cltcs_gap_summary.
-- The column lists are fixed in the cltcs_gap_item_ids and cltcs_gap_domains macros, so an
-- item or group added to cltcs_gap_items (or the cltcs_treatment_nice_indicators seed)
-- would otherwise be left out of the summary columns. Returns item and group codes only.
{% set gap_item_ids = cltcs_gap_item_ids() %}
{% set gap_domains = cltcs_gap_domains() %}

select item_id
    , score_domain
    , score_group
    , count(*) as row_count
from {{ ref('cltcs_gap_items') }}
where item_id not in (
        {%- for item_id in gap_item_ids %}
        '{{ item_id }}'{% if not loop.last %},{% endif %}
        {%- endfor %}
    )
    or score_domain || '.' || score_group not in (
        {%- for d in gap_domains %}
        {%- set outer_loop = loop %}
        {%- for group in d.groups %}
        '{{ d.domain }}.{{ group }}'{% if not (outer_loop.last and loop.last) %},{% endif %}
        {%- endfor %}
        {%- endfor %}
    )
group by item_id, score_domain, score_group

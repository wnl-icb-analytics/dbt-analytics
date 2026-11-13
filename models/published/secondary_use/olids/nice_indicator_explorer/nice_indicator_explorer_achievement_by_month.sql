{{ config(tags=['monthly-full', 'nice-history'], cluster_by=['indicator_id', 'breakdown', 'reporting_date']) }}

{{ nice_indicator_explorer_achievement('by_month') }}

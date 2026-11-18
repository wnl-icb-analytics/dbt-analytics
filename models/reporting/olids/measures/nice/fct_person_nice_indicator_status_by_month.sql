{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['indicator_id', 'reporting_date']) }}

{{ nice_indicator_status('by_month') }}

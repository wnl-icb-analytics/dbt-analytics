{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}
{{ nice_ind273('by_month') }}

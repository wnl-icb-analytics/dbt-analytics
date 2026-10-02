{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}
{{ nice_ind215('by_month') }}

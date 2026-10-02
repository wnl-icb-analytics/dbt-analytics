{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}
{{ nice_ind104('by_month') }}

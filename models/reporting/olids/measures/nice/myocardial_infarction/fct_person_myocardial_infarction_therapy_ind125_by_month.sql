{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind125('by_month') }}

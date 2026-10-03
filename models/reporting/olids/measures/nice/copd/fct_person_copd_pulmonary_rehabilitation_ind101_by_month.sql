{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind101('by_month') }}

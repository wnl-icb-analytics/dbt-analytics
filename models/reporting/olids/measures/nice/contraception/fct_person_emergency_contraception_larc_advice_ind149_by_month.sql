{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind149('by_month') }}

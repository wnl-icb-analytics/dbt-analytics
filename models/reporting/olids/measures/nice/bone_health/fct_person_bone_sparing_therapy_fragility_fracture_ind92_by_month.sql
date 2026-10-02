{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind92('by_month') }}

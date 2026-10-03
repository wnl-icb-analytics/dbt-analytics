{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind124('by_month') }}

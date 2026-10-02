{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind319('by_month') }}

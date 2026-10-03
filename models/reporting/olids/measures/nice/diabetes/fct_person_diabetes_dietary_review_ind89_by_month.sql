{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind89('by_month') }}

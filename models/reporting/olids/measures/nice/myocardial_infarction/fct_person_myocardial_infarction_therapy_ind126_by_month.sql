{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind126('by_month') }}

{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind316('by_month') }}

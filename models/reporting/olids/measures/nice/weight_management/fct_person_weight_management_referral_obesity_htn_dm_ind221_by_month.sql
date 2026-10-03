{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind221('by_month') }}

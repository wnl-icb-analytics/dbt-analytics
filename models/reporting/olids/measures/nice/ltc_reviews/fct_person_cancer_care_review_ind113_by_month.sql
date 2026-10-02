{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind113('by_month') }}

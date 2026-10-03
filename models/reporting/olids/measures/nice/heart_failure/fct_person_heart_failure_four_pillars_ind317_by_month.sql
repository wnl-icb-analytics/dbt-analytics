{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind317('by_month') }}

{{ config(materialized='view' , tags=['monthly-full', 'nice-history']) }}

{{ nice_ind318('by_month') }}

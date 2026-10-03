{{ config(materialized='view' , tags=['monthly-full', 'nice-history']) }}

{{ nice_ind260('by_month') }}

{{ config(materialized='table') }}

{{ nice_antithrombotic_therapy_union('current') }}

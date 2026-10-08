{{ config(materialized='table') }}

-- NICE IND247: https://www.nice.org.uk/indicators/ind247
-- DOAC order in 6 months, or a VKA order for valvular AF, antiphospholipid syndrome or a DOAC exception,
-- for people on the AF register with a latest CHA2DS2-VASc of 2 or more.
{{ nice_ind247('current') }}

-- depends_on: {{ ref('int_ltc_lcs_cf_dm_61_woEX') }}

{{ test_valuesets_have_codes(
    model=ref('int_ltc_lcs_cf_dm_61_woEX'),
    valuesets=['b7ce1049-b4c0-1477-ec22-7696854f03ed']
) }}

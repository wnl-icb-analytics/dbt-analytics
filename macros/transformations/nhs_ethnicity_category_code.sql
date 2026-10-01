{# Resolve a free-text or coded ethnicity value to the NHS Data Dictionary
   ETHNIC CATEGORY national code (A-S letters, R Chinese, S Other, Z Not stated,
   99 Not known). Mirrors the mapping proven on stg_mhcorl: accepts a bare
   letter code, explicit unknown/not-stated phrasings, and the 'White - X' /
   'Asian or Asian British - X' / bare-name free-text forms different providers
   submit. Unmappable values (local aggregations, blanks) -> NULL.
   `col` is a single ethnicity expression (coalesce siblings before passing). #}
{% macro nhs_ethnicity_category_code(col) %}
    case
        -- already a valid national code
        when upper(trim({{ col }})) in
            ('A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','Z','99','0')
            then upper(trim({{ col }}))

        -- explicit unknown / not stated
        when upper(trim({{ col }})) like 'NOT KNOWN%'                          then '99'
        when upper(trim({{ col }})) in ('UNKNOWN', 'INFORMATION NOT YET OBTAINED') then '99'
        when upper(trim({{ col }})) like 'NOT STATED%'                         then 'Z'
        when upper(trim({{ col }})) in ('REFUSED', 'NOT STATED')               then 'Z'

        -- White
        when upper(trim({{ col }})) in (
            'WHITE - BRITISH', 'WHITE BRITISH', 'BRITISH',
            'WHITE - ENGLISH', 'WHITE - WELSH', 'WHITE - SCOTTISH',
            'WHITE - NORTHERN IRISH', 'WHITE - CORNISH'
        ) then 'A'
        when upper(trim({{ col }})) in ('WHITE - IRISH', 'WHITE IRISH', 'IRISH') then 'B'
        when upper(trim({{ col }})) like 'WHITE%'
          or upper(trim({{ col }})) like 'ANY OTHER WHITE%'
          or upper(trim({{ col }})) = 'WHITE'
        then 'C'

        -- Mixed
        when upper(trim({{ col }})) in (
            'MIXED - WHITE & BLACK CARIBBEAN', 'MIXED - WHITE AND BLACK CARIBBEAN',
            'WHITE AND BLACK CARIBBEAN', 'MIXED WHITE AND BLACK CARIBBEAN'
        ) then 'D'
        when upper(trim({{ col }})) in (
            'MIXED - WHITE & BLACK AFRICAN', 'MIXED - WHITE AND BLACK AFRICAN',
            'WHITE AND BLACK AFRICAN', 'MIXED WHITE AND BLACK AFRICAN'
        ) then 'E'
        when upper(trim({{ col }})) in (
            'MIXED - WHITE & ASIAN', 'MIXED - WHITE AND ASIAN',
            'WHITE AND ASIAN', 'MIXED WHITE AND ASIAN'
        ) then 'F'
        when upper(trim({{ col }})) like 'MIXED%'
          or upper(trim({{ col }})) = 'MIXED'
          or upper(trim({{ col }})) like 'ANY OTHER MIXED%'
        then 'G'

        -- Asian
        when upper(trim({{ col }})) in (
            'ASIAN OR ASIAN BRITISH - INDIAN', 'ASIAN/ASIAN BRITISH INDIAN', 'INDIAN'
        ) then 'H'
        when upper(trim({{ col }})) in (
            'ASIAN OR ASIAN BRITISH - PAKISTANI', 'ASIAN/ASIAN BRITISH PAKISTANI', 'PAKISTANI'
        ) then 'J'
        when upper(trim({{ col }})) in (
            'ASIAN OR ASIAN BRITISH - BANGLADESHI', 'ASIAN/ASIAN BRITISH BANGLADESHI', 'BANGLADESHI'
        ) then 'K'
        when upper(trim({{ col }})) like 'ASIAN%'
          or upper(trim({{ col }})) = 'ASIAN'
          or upper(trim({{ col }})) like 'ANY OTHER ASIAN%'
        then 'L'

        -- Black
        when upper(trim({{ col }})) in (
            'BLACK OR BLACK BRITISH - CARIBBEAN', 'BLACK/BLACK BRITISH CARIBBEAN', 'CARIBBEAN'
        ) then 'M'
        when upper(trim({{ col }})) in (
            'BLACK OR BLACK BRITISH - AFRICAN', 'BLACK/BLACK BRITISH AFRICAN',
            'BLACK OR BLACK BRITISH - SOMALI', 'BLACK OR BLACK BRITISH - NIGERIAN',
            'AFRICAN'
        ) then 'N'
        when upper(trim({{ col }})) like 'BLACK%'
          or upper(trim({{ col }})) = 'BLACK'
          or upper(trim({{ col }})) like 'ANY OTHER BLACK%'
        then 'P'

        -- Other
        when upper(trim({{ col }})) in ('OTHER ETHNIC GROUPS - CHINESE', 'CHINESE') then 'R'
        when upper(trim({{ col }})) like 'OTHER%'
          or upper(trim({{ col }})) = 'OTHER'
          or upper(trim({{ col }})) like 'ANY OTHER%'
        then 'S'
    end
{% endmacro %}

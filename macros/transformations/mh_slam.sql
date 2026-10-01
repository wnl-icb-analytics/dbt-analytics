{# Shared building blocks for the MH Local SLAM staging models (stg_mh_nonapc,
   stg_mh_apc, stg_mh_referrals). The datasets share one SDL feed per source
   (MH for activity, REF for referrals) and are told apart only by the
   submission file name, so each model INNER JOINs a registry CTE filtered to
   its dataset's files. Spec: Appendix 2a-2c Mental Health Local SLAM Data
   Sets (2a Non-APC contacts, 2b APC ward stays, 2c Referrals). #}

{# META_FILE_REGISTRY rows for one dataset: feed + file-name regex. The join
   is the dataset filter (INNER), unlike the community feeds where the whole
   feed is one dataset. A regex, not LIKE: the '_' wildcard would let the APC
   pattern match the NONAPC token. original_file_name feeds the
   period-of-last-resort fallback. Join on (file_id, batch_id). #}
{% macro mh_slam_registry(feed, name_regex) %}
registry as (
    select file_id, batch_id, original_file_name
    from {{ ref('raw_sdl_wnl_meta_file_registry') }}
    where feed = '{{ feed }}'
      and original_file_name rlike '{{ name_regex }}'
)
{% endmacro %}

{# Gender identity -> MH national code (1 Male, 2 Female, 3 Non-binary,
   4 Other, X Not Known, Z Not Stated). Distinct from the community set
   (nc_gender), which has 9 Indeterminate and no 3/4/Z. Word variants mapped;
   valid codes and unrecognised values pass through trimmed. #}
{% macro nc_mh_gender_identity(col) %}
    case
        when upper(trim({{ col }})) in ('1', 'M', 'MALE')                    then '1'
        when upper(trim({{ col }})) in ('2', 'F', 'FEMALE')                  then '2'
        when upper(trim({{ col }})) in ('3', 'NON-BINARY', 'NON BINARY')     then '3'
        when upper(trim({{ col }})) in ('4', 'OTHER')                        then '4'
        when upper(trim({{ col }})) in ('X', '0', 'UNKNOWN', 'NOT KNOWN')    then 'X'
        when upper(trim({{ col }})) in ('Z', 'NOT STATED', 'NOT SPECIFIED')  then 'Z'
        else nullif(trim({{ col }}), '')
    end
{% endmacro %}

{# Clinical response priority -> MH national code (1 Emergency, 2 Urgent/
   serious, 3 Routine, 4 Very Urgent). NOT the community priority set, where
   Routine = 1 - do not reuse nc_priority here. Word variants mapped; codes
   and unrecognised values pass through trimmed. #}
{% macro nc_mh_priority(col) %}
    case
        when upper(trim({{ col }})) in ('1', 'EMERGENCY')                    then '1'
        when upper(trim({{ col }})) like 'VERY URGENT%'
          or upper(trim({{ col }})) = '4'                                    then '4'
        when upper(trim({{ col }})) like 'URGENT%'
          or upper(trim({{ col }})) = '2'                                    then '2'
        when upper(trim({{ col }})) in ('3', 'ROUTINE')                      then '3'
        else nullif(trim({{ col }}), '')
    end
{% endmacro %}

{# Normalise a numeric national code whose set is NOT fixed-width (e.g.
   consultation mechanism 1-13, 98; attend/DNA 2-7) by stripping leading
   zeros ('01' -> '1'); non-numeric values pass through trimmed. #}
{% macro nc_strip_zeros(col) %}
    case
        when trim({{ col }}) rlike '^0*[0-9]+$'
            then to_varchar(to_number(trim({{ col }})))
        else nullif(trim({{ col }}), '')
    end
{% endmacro %}

{# Registered GP practice: accept only a valid ODS shape (1 letter + 5 digits,
   optionally a 3-digit branch suffix which is stripped); other values -> NULL
   rather than truncated garbage. Same rule as the community PLD models. #}
{% macro clean_gp_practice_code(col) %}
    case
        when {{ col }} rlike '^[A-Za-z][0-9]{5}([0-9]{3})?$'
        then upper(left({{ col }}, 6))
    end
{% endmacro %}

{#
    Derives the A&E point of delivery (POD) band from HRG, patient group and
    department type.

    Translated from dbo.fnc_Update_POD_AE.

    The banding scheme changed at the start of financial year 2011/12, so the
    logic splits on activity date:
      before 2011-04-01 -> NO-PAY / HIGH / STANDARD / MINOR
      from   2011-04-01 -> NO-PAY / BAND 1..5

    Returns 'UNKNOWN' for an unrecognised HRG, and NULL when the activity date
    is NULL (the original's unreachable ELSE NULL branch).

    Usage:
      {{ derive_pod_ae('arrival_date', 'core_hrg', 'em_patient_group', 'em_department_type') }}
        as z_pod_level_4
#}

{% macro update_pod_ae(activity_date, ae_hrg, patient_group_code, department_type_code) %}
    {#-
        The UDF declared its parameters as varchar(6), so SQL Server silently
        truncated anything longer on the way in. left(..., 6) preserves that.
        upper() covers the case-insensitive collation the UDF relied on.
    -#}
    {%- set hrg = "upper(left(" ~ ae_hrg ~ ", 6))" -%}
    {%- set grp = "left(" ~ patient_group_code ~ ", 6)" -%}
    {%- set dept = "left(" ~ department_type_code ~ ", 6)" -%}

    case
        when {{ activity_date }} is null then null

        -- Pre 2011/12 scheme
        when to_date({{ activity_date }}) < '2011-04-01'::date then
            case
                when {{ hrg }} = 'U06' then 'NO-PAY'
                when {{ hrg }} in ('V01', 'V02', 'V03', 'V04') then 'HIGH'
                when {{ hrg }} in ('V07', 'V08') then 'STANDARD'
                when {{ hrg }} in ('V05', 'V06', 'DOA', 'V10', 'V100MC', 'V100MI')
                    or {{ grp }} = '70' then 'MINOR'
                else 'UNKNOWN'
            end

        -- 2011/12 onwards
        else
            case
                when {{ hrg }} = 'UZ01Z' then 'NO-PAY'
                when {{ hrg }} in ('VB01Z', 'VB02Z') then 'BAND 1'
                when {{ hrg }} in ('VB03Z', 'VB04Z', 'VB05Z') then 'BAND 2'
                when {{ hrg }} in ('VB06Z', 'VB09Z') then 'BAND 3'
                when {{ hrg }} in ('VB07Z', 'VB08Z', 'DOA')
                    or {{ grp }} = '70' then 'BAND 4'
                when {{ hrg }} in ('VB10Z', 'VB11Z', 'MIU')
                    or {{ dept }} = '03' then 'BAND 5'
                else 'UNKNOWN'
            end
    end
{% endmacro %}

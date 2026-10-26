{% macro get_ltc_lcs_bp_latest_by_setting(as_at='current_date()', ckd_pg2=false) %}
{# One row per person: latest LTC LCS blood pressure readings by setting, as at as_at.
   EMIS links BP components by calendar date before ranking eligible records by date
   (save time, then observation ID, break same-day ties). By default only the home-setting wrapper
   ranks before linking. Clinic/home pools use the CKD PG2 value-set instances
   shared by the HTN, PAD and Stroke chains.
   ckd_pg2 selects CKD's deepest systolic records before linking their parent
   diastolic pools and keeps its separate clinic/home setting chains.
   Diabetes retains its distinct BP_COD and component pools. No plausibility fallback applies. #}
with bp as (
    select
        person_id,
        observation_id,
        clinical_effective_date::date as bp_date,
        date_recorded,
        concept_code,
        result_value,
        boolor_agg(valueset_id = 'b0862f52-94d7-4831-9c3c-fa2cc8f8ca98') as is_clinic_marker,
        boolor_agg(valueset_id = 'b2592e74-731e-0d97-247a-ac3839913b70') as is_home_marker,
        boolor_agg(valueset_id = 'c2e9b41e-e9f9-5eee-f816-ee2543dddbdd' and result_value > 0) as is_systolic,
        boolor_agg(valueset_id = '3ddd36f2-7c5b-e43f-4620-46a0eb1283ee' and result_value > 0) as is_diastolic,
        boolor_agg(valueset_id = 'f27b2fe6-df28-de7d-5baa-089badef474d' and result_value > 0) as is_clinic_systolic,
        boolor_agg(valueset_id = '4c90ff98-51ec-2b89-e0c7-dc2ab98daed9' and result_value > 0) as is_clinic_diastolic,
        boolor_agg(valueset_id = '830a121c-275c-0ebc-990e-e9699b9875c4' and result_value > 0) as is_home_systolic,
        boolor_agg(valueset_id = '5aad8a47-dfac-2d96-493d-44fc69d61770' and result_value > 0) as is_home_diastolic,
        boolor_agg(valueset_id = '09a3d8a7-e54b-d710-9a86-58b3de69d1f4') as is_diabetes_marker,
        boolor_agg(valueset_id = '8da5f7b1-024b-c20c-97e9-415ea3b9a64a') as is_diabetes_link_systolic,
        boolor_agg(valueset_id = '5083eb15-91a9-f20e-7fb1-06e908a70e5f') as is_diabetes_systolic,
        boolor_agg(valueset_id = 'dbace9e1-ca62-3166-049c-82206d3e041d') as is_diabetes_diastolic
    from ({{ get_ltc_lcs_observations(
        "b0862f52-94d7-4831-9c3c-fa2cc8f8ca98,
         b2592e74-731e-0d97-247a-ac3839913b70,
         c2e9b41e-e9f9-5eee-f816-ee2543dddbdd,
         3ddd36f2-7c5b-e43f-4620-46a0eb1283ee,
         f27b2fe6-df28-de7d-5baa-089badef474d,
         4c90ff98-51ec-2b89-e0c7-dc2ab98daed9,
         830a121c-275c-0ebc-990e-e9699b9875c4,
         5aad8a47-dfac-2d96-493d-44fc69d61770,
         09a3d8a7-e54b-d710-9a86-58b3de69d1f4,
         8da5f7b1-024b-c20c-97e9-415ea3b9a64a,
         5083eb15-91a9-f20e-7fb1-06e908a70e5f,
         dbace9e1-ca62-3166-049c-82206d3e041d",
        include_date_recorded=true
    ) }})
    where clinical_effective_date::date <= {{ as_at }}
    group by person_id, observation_id, bp_date, date_recorded, concept_code, result_value
),

abpm_codes as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where cluster_id = 'ABPM_COD'
),
bp_days as (
    select
        person_id,
        bp_date,
        boolor_agg(is_clinic_marker) as has_clinic_marker,
        boolor_agg(is_home_marker) as has_home_marker,
        boolor_agg(is_systolic) as has_systolic,
        boolor_agg(is_clinic_systolic) as has_clinic_systolic,
        boolor_agg(is_home_systolic) as has_home_systolic,
        boolor_agg(is_diabetes_marker) as has_diabetes_marker,
        boolor_agg(is_diabetes_link_systolic) as has_diabetes_link_systolic
    from bp
    group by person_id, bp_date
),

latest_diastolic as (
    select person_id, bp_date, date_recorded, observation_id, is_clinic_marker
    from bp
    where is_diastolic
        {% if not ckd_pg2 %}
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days
            where has_systolic and (has_clinic_marker or has_home_marker)
        )
        {% endif %}
    qualify row_number() over (partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc) = 1
),

{% if ckd_pg2 %}
-- CKD's deepest systolic criterion ranks first. Its parent diastolic pool is
-- restricted to that date before ranking; setting checks remain separate.
latest_clinic_systolic as (
    select person_id, bp_date, date_recorded, observation_id, result_value
    from bp
    where is_clinic_systolic
    qualify row_number() over (
        partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc
    ) = 1
),

latest_clinic_diastolic as (
    select b.person_id, b.bp_date, b.date_recorded, b.observation_id, b.result_value
    from bp as b
    inner join latest_clinic_systolic as s on b.person_id = s.person_id and b.bp_date = s.bp_date
    where b.is_clinic_diastolic
    qualify row_number() over (
        partition by b.person_id order by b.date_recorded desc nulls last, b.observation_id desc
    ) = 1
),

latest_home_systolic as (
    select person_id, bp_date, date_recorded, observation_id, result_value
    from bp
    where is_home_systolic
    qualify row_number() over (
        partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc
    ) = 1
),

latest_home_diastolic as (
    select b.person_id, b.bp_date, b.date_recorded, b.observation_id, b.result_value,
        b.concept_code in (select code from abpm_codes) as is_abpm
    from bp as b
    inner join latest_home_systolic as s on b.person_id = s.person_id and b.bp_date = s.bp_date
    where b.is_home_diastolic
    qualify row_number() over (
        partition by b.person_id order by b.date_recorded desc nulls last, b.observation_id desc
    ) = 1
),

-- The CKD home wrapper links its clinic/home union to a valid home chain before
-- Latest 1. A later clinic reading therefore does not veto that home chain.
latest_home_setting_diastolic as (
    select person_id, bp_date
    from bp
    where is_home_diastolic
    qualify row_number() over (
        partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc
    ) = 1
),
{% else %}
latest_clinic_diastolic as (
    select person_id, bp_date, date_recorded, observation_id, result_value
    from bp
    where is_clinic_diastolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days where has_clinic_systolic and has_clinic_marker
        )
    qualify row_number() over (partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc) = 1
),

latest_clinic_systolic as (
    select b.person_id, b.bp_date, b.date_recorded, b.observation_id, b.result_value
    from bp as b
    inner join latest_clinic_diastolic as d on b.person_id = d.person_id and b.bp_date = d.bp_date
    where b.is_clinic_systolic
    qualify row_number() over (
        partition by b.person_id order by b.date_recorded desc nulls last, b.observation_id desc
    ) = 1
),

latest_home_diastolic as (
    select person_id, bp_date, date_recorded, observation_id, result_value,
        concept_code in (select code from abpm_codes) as is_abpm
    from bp
    where is_home_diastolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days where has_home_systolic and has_home_marker
        )
    qualify row_number() over (partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc) = 1
),

latest_home_systolic as (
    select b.person_id, b.bp_date, b.date_recorded, b.observation_id, b.result_value
    from bp as b
    inner join latest_home_diastolic as d on b.person_id = d.person_id and b.bp_date = d.bp_date
    where b.is_home_systolic
    qualify row_number() over (
        partition by b.person_id order by b.date_recorded desc nulls last, b.observation_id desc
    ) = 1
),

{% endif %}

latest_diabetes_systolic as (
    select person_id, bp_date, result_value
    from bp
    where is_diabetes_systolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days where has_diabetes_marker and has_diabetes_link_systolic
        )
    qualify row_number() over (
        partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc
    ) = 1
),

latest_diabetes_diastolic as (
    select person_id, bp_date, result_value
    from bp
    where is_diabetes_diastolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days where has_diabetes_marker and has_diabetes_link_systolic
        )
    qualify row_number() over (
        partition by person_id order by bp_date desc, date_recorded desc nulls last, observation_id desc
    ) = 1
),
people as (
    select distinct person_id from bp
)

select
    p.person_id,
    -- Clinic setting tests clinic coding and systolic evidence on the selected diastolic date.
    ld.bp_date as latest_reading_date,
    coalesce(ld.is_clinic_marker and ldd.has_systolic, false) as is_latest_reading_clinic,
    {% if ckd_pg2 %}
    coalesce(hsd.has_home_marker and hsd.has_home_systolic, false) as is_latest_reading_home,
    {% else %}
    -- Home setting: latest home reading is valid and no valid clinic reading is later.
    coalesce(
        lhs.bp_date = lhd.bp_date
        and lhdd.has_home_marker
        and (
            lcd.bp_date is null
            or not (lcdd.has_clinic_systolic and lcdd.has_clinic_marker)
            or lcd.bp_date < lhd.bp_date
            or (lcd.bp_date = lhd.bp_date and (
                lcd.date_recorded < lhd.date_recorded
                or (lcd.date_recorded is null and lhd.date_recorded is not null)
                or (equal_null(lcd.date_recorded, lhd.date_recorded)
                    and lcd.observation_id <= lhd.observation_id)
            ))
        ),
        false
    ) as is_latest_reading_home,
    {% endif %}
    -- Clinic components and marker must share the selected calendar date.
    iff(lcd.bp_date = lcs.bp_date and lcsd.has_clinic_marker, lcs.bp_date, null) as clinic_reading_date,
    iff(lcd.bp_date = lcs.bp_date and lcsd.has_clinic_marker, lcs.result_value, null) as clinic_systolic_value,
    iff(lcd.bp_date = lcs.bp_date and lcsd.has_clinic_marker, lcd.result_value, null) as clinic_diastolic_value,
    -- Home reading: same rule with the home sets.
    iff(lhd.bp_date = lhs.bp_date and lhsd.has_home_marker, lhs.bp_date, null) as home_reading_date,
    iff(lhd.bp_date = lhs.bp_date and lhsd.has_home_marker, lhs.result_value, null) as home_systolic_value,
    iff(lhd.bp_date = lhs.bp_date and lhsd.has_home_marker, lhd.result_value, null) as home_diastolic_value,
    coalesce(lhd.is_abpm, false) as is_home_reading_abpm,
    -- Diabetes ranks each component among dates with BP_COD and linked systolic evidence.
    iff(dss.has_diabetes_marker and dss.has_diabetes_link_systolic, ds.result_value, null) as diabetes_systolic_value,
    iff(dsd.has_diabetes_marker and dsd.has_diabetes_link_systolic, dd.result_value, null) as diabetes_diastolic_value
from people as p
left join latest_diastolic as ld on ld.person_id = p.person_id
left join bp_days as ldd on ldd.person_id = ld.person_id and ldd.bp_date = ld.bp_date
left join latest_clinic_systolic as lcs on lcs.person_id = p.person_id
left join bp_days as lcsd on lcsd.person_id = lcs.person_id and lcsd.bp_date = lcs.bp_date
left join latest_clinic_diastolic as lcd on lcd.person_id = p.person_id
left join bp_days as lcdd on lcdd.person_id = lcd.person_id and lcdd.bp_date = lcd.bp_date
left join latest_home_systolic as lhs on lhs.person_id = p.person_id
left join bp_days as lhsd on lhsd.person_id = lhs.person_id and lhsd.bp_date = lhs.bp_date
left join latest_home_diastolic as lhd on lhd.person_id = p.person_id
left join bp_days as lhdd on lhdd.person_id = lhd.person_id and lhdd.bp_date = lhd.bp_date
{% if ckd_pg2 %}
left join latest_home_setting_diastolic as hsdia on hsdia.person_id = p.person_id
left join bp_days as hsd on hsd.person_id = hsdia.person_id and hsd.bp_date = hsdia.bp_date
{% endif %}
left join latest_diabetes_systolic as ds on ds.person_id = p.person_id
left join bp_days as dss on dss.person_id = ds.person_id and dss.bp_date = ds.bp_date
left join latest_diabetes_diastolic as dd on dd.person_id = p.person_id
left join bp_days as dsd on dsd.person_id = dd.person_id and dsd.bp_date = dd.bp_date
{% endmacro %}

{% macro calculate_nice_cvd_secondary_prevention_population(reference='current') %}
{#-
    Calculate CVD register membership, diagnosis anchors and lifelong exclusions.
    Args: reference is current or by_month; register adapters define membership.
    Returns: person_id, reporting_date, CHD/STIA/PAD flags and earliest dates,
             earliest_cvd_diagnosis_date, FH and haemorrhagic-stroke history flags.
    Current candidates are live register members; monthly candidates are active
    monthly register members. Indicators exclude test patients through population joins.
-#}
WITH cvd_registers AS (
    {% for condition in ['CHD', 'STIA', 'PAD'] %}
    SELECT
        person_id,
        reporting_date,
        condition_code,
        earliest_diagnosis_date::DATE AS earliest_diagnosis_date
    FROM ({{ nice_register(condition, reference) }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

cvd_people AS (
    SELECT
        person_id,
        reporting_date,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stroke_tia,
        BOOLOR_AGG(condition_code = 'PAD') AS has_pad,
        MIN(IFF(condition_code = 'CHD', earliest_diagnosis_date, NULL)) AS earliest_chd_diagnosis_date,
        MIN(IFF(condition_code = 'STIA', earliest_diagnosis_date, NULL)) AS earliest_stroke_tia_diagnosis_date,
        MIN(IFF(condition_code = 'PAD', earliest_diagnosis_date, NULL)) AS earliest_pad_diagnosis_date,
        MIN(earliest_diagnosis_date) AS earliest_cvd_diagnosis_date
    FROM cvd_registers
    WHERE earliest_diagnosis_date <= reporting_date
    GROUP BY person_id, reporting_date
),

fh_history AS (
    SELECT
        person_id,
        MIN(clinical_effective_date_raw::DATE) AS first_date,
        BOOLOR_AGG(clinical_effective_date_raw IS NULL) AS has_undated_record
    FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }}
    WHERE is_diagnosis_code
    GROUP BY person_id
)
SELECT
    cvd.person_id,
    cvd.reporting_date,
    cvd.has_chd,
    cvd.has_stroke_tia,
    cvd.has_pad,
    cvd.earliest_chd_diagnosis_date,
    cvd.earliest_stroke_tia_diagnosis_date,
    cvd.earliest_pad_diagnosis_date,
    cvd.earliest_cvd_diagnosis_date,
    COALESCE(fh.has_undated_record OR fh.first_date <= cvd.reporting_date, FALSE) AS has_familial_hypercholesterolaemia,
    COALESCE(hs.has_undated_record OR hs.earliest_recorded_date <= cvd.reporting_date, FALSE) AS has_haemorrhagic_stroke
FROM cvd_people AS cvd
LEFT JOIN fh_history AS fh
    ON cvd.person_id = fh.person_id
LEFT JOIN {{ ref('int_haemorrhagic_stroke_history') }} AS hs
    ON cvd.person_id = hs.person_id
{% endmacro %}

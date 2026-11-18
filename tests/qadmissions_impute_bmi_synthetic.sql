{{ config(tags=['qadmissions']) }}

-- Synthetic check of the QAdmissions BMI imputation macro
-- (macros/clinical/qadmissions_impute_bmi.sql). Returns a row for every
-- result that differs from the expected one. All values are made up.
--
-- Runs as a test of qadmissions_input_features, so a failure stops scoring.
-- depends_on: {{ ref('qadmissions_input_features') }}
--
-- min_group is 2 so the groups can be small. At 2023-01-31:
--   a: Male 30s White has 2 recorded BMIs (20, 30)      -> 25, own group
--   b: Male 30s Asian has 1 recorded BMI                -> Male 30s mean, 30
--   c: Male 90s has no recorded BMIs                    -> Male mean, 35
--   f: Female 20s White mean is 46, outside 20-40       -> 46, not clipped
-- At 2024-12-31, a's group mean is 105: index dates must not mix.

WITH bmi_imputation_input AS (
    SELECT
        column1::DATE    AS end_date,
        column2::VARCHAR AS person_id,
        column3::VARCHAR AS sex,
        column4::NUMBER  AS age_decade,
        column5::VARCHAR AS ethnicity_group,
        column6::FLOAT   AS bmi_recorded
    FROM VALUES
        ('2023-01-31', 'r1', 'Male',   30, 'White', 20),
        ('2023-01-31', 'r2', 'Male',   30, 'White', 30),
        ('2023-01-31', 'r3', 'Male',   30, 'Asian', 40),
        ('2023-01-31', 'r4', 'Male',   50, 'White', 50),
        ('2023-01-31', 'a',  'Male',   30, 'White', NULL),
        ('2023-01-31', 'b',  'Male',   30, 'Asian', NULL),
        ('2023-01-31', 'c',  'Male',   90, 'Black', NULL),
        ('2023-01-31', 'f1', 'Female', 20, 'White', 45),
        ('2023-01-31', 'f2', 'Female', 20, 'White', 47),
        ('2023-01-31', 'f',  'Female', 20, 'White', NULL),
        ('2024-12-31', 'r1', 'Male',   30, 'White', 100),
        ('2024-12-31', 'r2', 'Male',   30, 'White', 110),
        ('2024-12-31', 'a',  'Male',   30, 'White', NULL)
),

expected AS (
    SELECT
        column1::DATE    AS end_date,
        column2::VARCHAR AS person_id,
        column3::FLOAT   AS expected_bmi,
        column4::BOOLEAN AS expected_is_imputed,
        column5::VARCHAR AS expected_group
    FROM VALUES
        ('2023-01-31', 'r1', 20,  FALSE, NULL),
        ('2023-01-31', 'r2', 30,  FALSE, NULL),
        ('2023-01-31', 'r3', 40,  FALSE, NULL),
        ('2023-01-31', 'r4', 50,  FALSE, NULL),
        ('2023-01-31', 'a',  25,  TRUE,  'sex_age_ethnicity'),
        ('2023-01-31', 'b',  30,  TRUE,  'sex_age'),
        ('2023-01-31', 'c',  35,  TRUE,  'sex'),
        ('2023-01-31', 'f1', 45,  FALSE, NULL),
        ('2023-01-31', 'f2', 47,  FALSE, NULL),
        ('2023-01-31', 'f',  46,  TRUE,  'sex_age_ethnicity'),
        ('2024-12-31', 'r1', 100, FALSE, NULL),
        ('2024-12-31', 'r2', 110, FALSE, NULL),
        ('2024-12-31', 'a',  105, TRUE,  'sex_age_ethnicity')
),

imputed AS (
    {{ qadmissions_impute_bmi('bmi_imputation_input', partition_by=['end_date'], min_group=2) }}
),

switched_off AS (
    {{ qadmissions_impute_bmi('bmi_imputation_input', partition_by=['end_date'], min_group=2, enabled=false) }}
),

ethnicity_mapping AS (
    SELECT
        column1::VARCHAR AS ethnicity_category,
        column2::VARCHAR AS expected_group
    FROM VALUES
        ('Unknown',      'Not Recorded'),
        (NULL,           'Not Recorded'),
        ('Not Recorded', 'Not Recorded'),
        ('Asian',        'Asian')
)

-- Imputation: every expected row, with the expected values, and no extra rows.
SELECT
    'imputation'                                    AS check_name,
    COALESCE(e.end_date, i.end_date)                AS end_date,
    COALESCE(e.person_id, i.person_id)              AS person_id,
    'bmi ' || COALESCE(i.bmi::VARCHAR, 'NULL')
        || ', imputed ' || COALESCE(i.is_bmi_imputed::VARCHAR, 'NULL')
        || ', group ' || COALESCE(i.bmi_imputation_group, 'NULL')
        || '; expected ' || COALESCE(e.expected_bmi::VARCHAR, 'no row')
        || ', ' || COALESCE(e.expected_is_imputed::VARCHAR, '-')
        || ', ' || COALESCE(e.expected_group, 'NULL') AS detail
FROM expected AS e
FULL OUTER JOIN imputed AS i
    ON e.end_date = i.end_date AND e.person_id = i.person_id
WHERE NOT EQUAL_NULL(ROUND(i.bmi, 6), ROUND(e.expected_bmi, 6))
   OR NOT EQUAL_NULL(i.is_bmi_imputed, e.expected_is_imputed)
   OR NOT EQUAL_NULL(i.bmi_imputation_group, e.expected_group)

UNION ALL

-- Switched off: bmi is the recorded value and nothing is flagged.
SELECT
    'switched off',
    end_date,
    person_id,
    'bmi ' || COALESCE(bmi::VARCHAR, 'NULL')
        || ', imputed ' || is_bmi_imputed::VARCHAR
        || ', group ' || COALESCE(bmi_imputation_group, 'NULL')
FROM switched_off
WHERE NOT EQUAL_NULL(bmi, bmi_recorded)
   OR is_bmi_imputed
   OR bmi_imputation_group IS NOT NULL

UNION ALL

-- Ethnicity groups: 'Unknown' and a missing category become 'Not Recorded'.
SELECT
    'ethnicity mapping',
    NULL,
    NULL,
    COALESCE(ethnicity_category, 'NULL') || ' -> '
        || COALESCE({{ qadmissions_bmi_ethnicity_group('ethnicity_category') }}, 'NULL')
FROM ethnicity_mapping
WHERE NOT EQUAL_NULL({{ qadmissions_bmi_ethnicity_group('ethnicity_category') }}, expected_group)

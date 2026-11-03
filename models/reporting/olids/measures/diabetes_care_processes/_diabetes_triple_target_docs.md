{% docs diabetes_triple_target_rules %}
Diabetes treatment targets aligned with the National Diabetes Audit (NDA). The population is people registered and alive at the reporting date on the QOF diabetes register (aged 17 or over). The audit period is the 15 calendar months ending in the reporting month, matching the NDA's 1 January to 31 March period at a March reporting date. All three targets are met when the latest HbA1c in the period is valid and at or below 58 mmol/mol, the latest blood pressure in the period is valid with systolic at or below 140 and diastolic at or below 80 mmHg, and, where a statin is indicated, a statin is prescribed in the period. A statin is indicated for people aged 40 to 80, or of any age with CVD. People with no reading in the period do not meet that target, and an invalid latest reading is not replaced by an older one. Local differences from the NDA: CVD comes from the GP CHD, stroke/TIA and PAD registers rather than hospital admissions; a same-day HbA1c tie prefers a valid result, then the highest observation id, rather than the lowest value; the NDA's under-12 HbA1c-only rule does not apply because the register starts at 17.
{% enddocs %}

{% docs diabetes_triple_target_measurement_period_start %}
Inclusive start of the audit period: the first day of the month 14 months before the reporting month.
{% enddocs %}

{% docs diabetes_triple_target_nda_diabetes_type %}
NDA reporting group: Type 1, or Type 2 and other (Type 2, untyped and other diabetes types).
{% enddocs %}

{% docs diabetes_triple_target_latest_hba1c_date %}
Date of the latest HbA1c test in the audit period, including tests with missing or invalid results; null when none is recorded.
{% enddocs %}

{% docs diabetes_triple_target_latest_hba1c_value %}
Latest HbA1c in the audit period in IFCC mmol/mol, converted from DCCT % where needed. Can be present but invalid; read with is_latest_hba1c_valid.
{% enddocs %}

{% docs diabetes_triple_target_hba1c_in_target_range %}
Latest HbA1c in the audit period is valid and at or below 58 mmol/mol. False when none is recorded.
{% enddocs %}

{% docs diabetes_triple_target_latest_bp_date %}
Date of the latest complete blood pressure pair in the audit period; null when none is recorded. Same-day readings use the lowest valid pair.
{% enddocs %}

{% docs diabetes_triple_target_bp_in_target_range %}
Latest blood pressure in the audit period is valid with systolic at or below 140 and diastolic at or below 80 mmHg. False when none is recorded.
{% enddocs %}

{% docs diabetes_triple_target_has_cvd %}
On the CHD, stroke/TIA or PAD register at the reporting date. Local proxy for the NDA's history of hospital admission for CVD.
{% enddocs %}

{% docs diabetes_triple_target_is_statin_indicated %}
In the NDA combined CVD prevention group: aged 40 to 80, or any age with CVD.
{% enddocs %}

{% docs diabetes_triple_target_is_statin_target_met %}
Statin prescribed in the audit period for people with a statin indicated; null when a statin is not indicated.
{% enddocs %}

{% docs diabetes_triple_target_all_three_targets_met %}
HbA1c and blood pressure targets met and, where a statin is indicated, a statin prescribed in the audit period.
{% enddocs %}

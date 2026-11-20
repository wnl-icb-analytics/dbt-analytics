{% macro cltcs_gap_item_ids() %}
  {#
    Item IDs that cltcs_gap_summary turns into one is_gap_<item_id> column each. NICE
    indicators follow cltcs_treatment_nice_indicators; bespoke items follow cltcs_gap_items.
    Keep this list in step with both: the cltcs_gap_summary_covers_gap_items test fails when
    cltcs_gap_items produces an item that is not listed here.
  #}
  {{ return([
    'IND179', 'IND180', 'IND239', 'IND240', 'IND241', 'IND242', 'IND243', 'IND244',
    'IND245', 'IND246', 'IND235', 'IND264', 'IND249', 'IND278', 'IND87',
    'IND144', 'IND139', 'IND86', 'IND169', 'IND191', 'IND195', 'IND273', 'IND315',
    'IND142', 'IND143', 'IND248', 'IND265', 'IND110', 'IND223',
    'IND128', 'IND247', 'IND132', 'IND133', 'IND94', 'IND125', 'IND126', 'IND230',
    'IND229', 'IND130', 'IND263', 'IND324', 'IND231', 'IND134', 'IND275', 'IND317',
    'IND91', 'IND92', 'IND101', 'IND316', 'IND155', 'IND157', 'IND200', 'IND202',
    'IND220', 'IND221', 'IND207', 'IND232',
    'EGFR_OVERDUE',
    'DM_CP_CHOLESTEROL', 'DM_CP_CREATININE', 'DM_CP_ACR', 'DM_CP_FOOT_CHECK',
    'DM_CP_BMI', 'DM_CP_SMOKING', 'DM_CP_RETINAL_SCREENING',
    'HBA1C_ABOVE_90', 'ASTHMA_SALBUTAMOL_ONLY', 'COPD_EXACERBATIONS',
    'ASTHMA_STEROID_COURSES', 'ASTHMA_SALBUTAMOL_OVERUSE'
  ]) }}
{% endmacro %}


{% macro cltcs_gap_domains() %}
  {#
    Score domains and the groups within each that cltcs_gap_summary turns into columns.
    stem is the column prefix for the domain (has_gap_<stem>_<group>, <stem>_groups_assessed).
    Monitoring includes the disease control groups because a disease control result that
    was not recorded is a monitoring gap in the same group.
  #}
  {{ return([
    {'domain': 'disease_control', 'stem': 'disease_control', 'groups': [
        'diabetes', 'blood_pressure', 'lipids', 'lithium', 'respiratory'
    ]},
    {'domain': 'monitoring_gaps', 'stem': 'monitoring', 'groups': [
        'diabetes', 'blood_pressure', 'lipids', 'lithium', 'ckd', 'hypothyroidism',
        'atrial_fibrillation', 'copd', 'heart_failure', 'asthma', 'dementia', 'smi',
        'learning_disability', 'rheumatoid_arthritis', 'cancer'
    ]},
    {'domain': 'treatment_optimisation', 'stem': 'treatment_optimisation', 'groups': [
        'atrial_fibrillation', 'cvd', 'cvd_prevention', 'ckd', 'diabetes', 'heart_failure',
        'bone_health', 'copd', 'asthma', 'smoking', 'alcohol', 'weight',
        'medication_review', 'nsaid_safety'
    ]}
  ]) }}
{% endmacro %}

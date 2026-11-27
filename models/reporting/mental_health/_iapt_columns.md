{% docs iapt_nhse_therapy_course_count %}
Supplied NHS England derived course total by PathwayID, following the therapy-specific rule in I101D34-I101D52. Totals survive refreshes that drop historical contacts, so they can exceed counts reconstructed from retained contact rows. A transfer successor retaining its predecessor's PathwayID carries cumulative totals including pre-transfer care; do not add them to the predecessor's totals. A successor with a new PathwayID covers care after transfer only. See is_pathway_id_retained in fct_iapt_referral_summary. Zero means no qualifying sessions; null means no total was supplied. Do not sum monthly history versions of the same referral.
{% enddocs %}

{% docs iapt_age_at_assessment %}
Supplied NHS England age in completed years (Age_AssessmentCompletion_Date). IDS606 uses assessment completion date (I606D07); IDS607 uses the related care contact date (I607D09). Retained as supplied, including when the fact cannot validate its clinical date or parent link. Null when no age is supplied.
{% enddocs %}

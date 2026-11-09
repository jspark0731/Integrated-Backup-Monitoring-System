"""STEP 4A's 29-field contract, isolated shadow resource only."""

SHADOW_INDEX = "poc-nw-s4c2-action-lookup"


def lookup_schema():
    keywords = """action_run_key source_networker action_job_id action_job_type
        action_status action_workflow_run_key policy_name workflow_name protection_group
        mapping_status verification_status evidence_state reason normalization_version
        suffix_validation lookup_generation candidate_fingerprint""".split()
    dates = "action_start_time action_end_time lookup_observed_at lookup_synced_at coverage_from coverage_to".split()
    properties = {name: {"type": "keyword"} for name in keywords}
    properties.update({name: {"type": "date"} for name in dates})
    properties.update({name: {"type": "long"} for name in
                       ["candidate_count", "raw_candidate_count", "backup_reference_count"]})
    properties["backup_reference_truncated"] = {"type": "boolean"}
    properties.update({name: {"type": "object", "enabled": False}
                       for name in ["backup_references", "candidate_evidence"]})
    return {"settings": {"index.mode": "lookup", "number_of_shards": 1,
                         "index.mapping.ignore_malformed": False},
            "mappings": {"dynamic": False, "properties": properties}}

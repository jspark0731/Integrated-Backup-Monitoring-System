"""Synthetic Workflow/current integration, with an in-memory Elasticsearch transport."""
from copy import deepcopy
from datetime import datetime, timezone
import fnmatch
import json
from pathlib import Path

from elastic_transport import ApiResponseMeta, NodeConfig
from elasticsearch import ConflictError, NotFoundError
from elasticsearch.helpers import BulkIndexError
import pytest
import yaml

from apps.core.config import ElasticsearchConfig, NetworkerCurrentConfig, load_config
from apps.models import CollectionResult
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.processors.networker_current import build_daily, bucket, closed_days, timestamp, window
from apps.writers.elasticsearch import ElasticsearchWriter
from apps.writers.networker_current import CurrentReadError, CurrentSnapshot, NetworkerDailyCurrent, read_jobs

DAY = "2026-11-11"
NOW = datetime(2026, 11, 12, 0, 10, tzinfo=timezone.utc)
START = "2026-11-11T12:00:00+00:00"
END = "2026-11-11T12:20:00+00:00"


def job(identity=7100, **changes):
    return {"id": identity, "type": "workflow job", "name": "SYNTH_POLICY", "workflowName": "SYNTH_W",
            "state": "Completed", "completionStatus": "Succeeded", "exitCode": 0,
            "startTime": START, "endTime": END, "parentJobId": 0, "rootParentJobId": 0} | changes


def result(jobs, source="core", collected=NOW):
    raw = {"jobs": {"jobs": jobs}}
    parsed = parse_networker_rest_payload(raw, "networker_" + source, source_networker=source)
    return CollectionResult(collector="networker_" + source, target_type="Networker", protocol="rest",
                            collected_at=collected, ok=True, collection_class="jobs",
                            payload=parsed | {"raw": raw, "collection_status": "success"})


def hits(jobs, source="core", collected=NOW):
    writer = ElasticsearchWriter(ElasticsearchConfig())
    return [{k: a[k] for k in ("_index", "_id", "_source")} for a in writer._actions_for_result(result(jobs, source, collected))
            if a["_source"].get("document_type") == "job-run"]


def proof(**changes):
    return {"start_time": START, "reference": "SYNTHETIC execution evidence",
            "workflow_kind": "backup", "attempted": True, "warning": False} | changes


def config(evidence=None, **changes):
    return NetworkerCurrentConfig(enabled=True, rebuild_days=1, run_evidence=evidence or {},
                                  coverage={"core:" + DAY: "SYNTHETIC full fixture coverage"}, **changes)


def rule(**changes):
    return {"source_networker": "core", "policy_name": "SYNTH_POLICY", "workflow_name": "SYNTH_W",
            "reference": "SYNTHETIC reviewed Workflow operating setting",
            "effective_from": "2026-11-01T00:00:00Z"} | changes


def build(jobs, evidence=None, source="core", now=NOW, rules=()):
    cfg = config(evidence, workflow_rules=rules)
    return build_daily(hits(jobs, source), source, closed_days(now, source, cfg), now, cfg)


def skip():
    return ("Request to run the enabled action 'backup' that has level configured as 'skip'.\n"
            "Not running action 'backup' because either the schedule says to skip it today, "
            "or, the previously failed action was not restarted within the specified restart window.")


def hierarchy():
    return [job(), job(7200, type="backup action job", parentJobId=7100, rootParentJobId=7100,
                       startTime="2026-11-11T12:01:00Z"),
            job(7300, type="save job", parentJobId=7200, rootParentJobId=7100,
                startTime="2026-11-11T12:05:00Z", endTime="2026-11-11T12:10:00Z")]


def test_real_hierarchy_proves_activity_but_only_workflow_result_determines_success():
    jobs = hierarchy()
    jobs[-1].update(completionStatus="Failed", exitCode=1)
    summary, rows = build(jobs)[DAY]
    assert len(rows) == 1 and rows[0]["dashboard_result"] == "success"
    assert summary["success_count"] == 1 and summary["failure_count"] == 0
    assert summary["warning_success_count"] is None
    assert summary["data_status"] == "partial"  # Warning evidence was not invented.


def test_workflow_failure_is_not_replaced_by_successful_save():
    jobs = hierarchy()
    jobs[0].update(completionStatus="Failed", exitCode=1)
    summary, rows = build(jobs)[DAY]
    assert rows[0]["dashboard_result"] == "failed" and summary["failure_count"] == 1


def test_unknown_child_type_does_not_erase_explicit_workflow_failure():
    jobs = [job(completionStatus="Failed"), job(7200, type="synthetic unsupported phase", rootParentJobId=7100)]
    summary, rows = build(jobs)[DAY]
    assert summary["failure_count"] == 1 and rows[0]["dashboard_result"] == "failed"
    assert summary["data_status"] == "partial"
    assert "unsupported_auxiliary_job_not_used_for_outcome" in rows[0]["analysis_reasons"]


def test_explicit_backup_skip_excludes_without_manual_evidence():
    jobs = [job(message=skip())]
    summary, rows = build(jobs)[DAY]
    assert rows[0]["dashboard_result"] == "excluded"
    assert rows[0]["source_completion_status"] == "Succeeded"
    assert rows[0]["source_status_normalized"] == "success"
    assert summary["success_count"] == summary["failure_count"] == 0
    assert summary["unconfirmed_workflow_count"] == 0
    assert rows[0]["result_detail"] == "skipped"
    assert summary["excluded_workflow_count"] == 1 and summary["success_rate"] is None
    assert hits(jobs)[0]["_source"]["payload"]["message"] == skip()


@pytest.mark.parametrize("message", [
    "Nothing to skip; backup completed.",
    "Consider starting action 'backup', enabled 1, schedule action 'skip'",
    "Request to run the enabled action 'backup' that has level configured as 'skip'.",
    "Not running action 'backup' because either the schedule says to skip it today.",
    skip().replace("'backup'", "'notify'"),
    skip().replace("'backup'", "'clone'"),
    "Save target '/synthetic' skipped; remaining targets completed.",
    "Request to run the enabled action 'backup' that has level configured as 'full'.\n"
    "Not running action 'notify' because either the schedule says to skip it today.",
])
def test_partial_or_other_skip_messages_do_not_exclude_workflow(message):
    summary, rows = build([job(message=message)])[DAY]
    assert rows[0]["dashboard_result"] == "success" and summary["success_count"] == 1
    assert summary["excluded_workflow_count"] == 0


@pytest.mark.parametrize("completion,state,detail", [
    ("Failed", "Completed", "failed"), ("Canceled", "Completed", "canceled"),
    ("Aborted", "Completed", "aborted"), ("Abandoned", "Completed", "aborted"),
    ("Unknown", "Completed", "needs_attention"), ("", "Abandoned", "aborted"),
])
def test_terminal_failure_with_backup_skip_remains_failure(completion, state, detail):
    summary, rows = build([job(completionStatus=completion, state=state, message=skip())])[DAY]
    assert rows[0]["dashboard_result"] == "failed" and rows[0]["result_detail"] == detail
    assert rows[0]["source_completion_status"] == (completion or None)
    assert summary["failure_count"] == 1 and summary["excluded_workflow_count"] == 0


@pytest.mark.parametrize("outcome,expected", [("Succeeded", "success"), ("Failed", "failed")])
def test_skip_cannot_replace_last_real_rerun_outcome_without_manual_evidence(outcome, expected):
    jobs = [job(completionStatus="Failed"),
            job(7101, completionStatus=outcome, startTime="2026-11-11T13:00:00Z",
                endTime="2026-11-11T13:10:00Z"),
            job(7102, message=skip(), startTime="2026-11-11T14:00:00Z", endTime="2026-11-11T14:01:00Z")]
    summary, rows = build(jobs)[DAY]
    assert rows[0]["workflow_job_id"] == "7101" and rows[0]["dashboard_result"] == expected
    assert summary["success_count"] == (expected == "success")
    assert summary["failure_count"] == (expected == "failed")
    assert rows[0]["execution_results"][-1]["result_detail"] == "skipped"


def test_skip_with_executed_children_cannot_exclude_whole_workflow():
    jobs = hierarchy()
    jobs[0]["message"] = skip()
    summary, rows = build(jobs)[DAY]
    assert rows[0]["dashboard_result"] == "success"
    assert "skip_and_execution_evidence_conflict" in rows[0]["analysis_reasons"]
    assert summary["excluded_workflow_count"] == 0


def test_backup_skip_with_another_action_child_does_not_exclude_entire_execution():
    jobs = [job(message=skip()), job(7200, type="synthetic auxiliary action", parentJobId=7100,
        rootParentJobId=7100, startTime="2026-11-11T12:01:00Z")]
    summary, rows = build(jobs)[DAY]
    assert rows[0]["dashboard_result"] == "success" and summary["success_count"] == 1
    assert summary["excluded_workflow_count"] == 0
    assert "skip_and_execution_evidence_conflict" in rows[0]["analysis_reasons"]


def test_skip_control_action_document_is_not_itself_backup_activity():
    jobs = [job(message=skip()), job(7200, type="backup action job", parentJobId=7100,
            rootParentJobId=7100, startTime="2026-11-11T12:01:00Z", message=skip())]
    summary, rows = build(jobs, {"core:7100": proof(target_actions=["backup"], attempted=False)})[DAY]
    assert rows[0]["result_detail"] == "skipped" and summary["excluded_workflow_count"] == 1
    summary, rows = build(jobs, {"core:7100": proof(target_actions=["backup"], attempted=True)})[DAY]
    assert rows[0]["result_detail"] == "skipped" and summary["excluded_workflow_count"] == 1


def test_failed_then_success_is_one_result_with_selected_execution_duration():
    jobs = [job(7100, completionStatus="Failed", exitCode=1),
            job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:10:00Z")]
    evidence = {"core:7101": proof(start_time=jobs[1]["startTime"])}
    summary, rows = build(jobs, evidence)[DAY]
    assert summary["success_count"] == 1 and summary["failure_count"] == 0
    assert rows[0]["workflow_job_id"] == "7101" and rows[0]["observed_execution_count"] == 2
    assert summary["duration_seconds_sum"] == summary["duration_seconds_average"] == 600
    assert summary["duration_sample_count"] == 1
    assert summary["last_actual_success_at"] == "2026-11-11T13:10:00+00:00"


@pytest.mark.parametrize("completion,detail", [("Canceled", "canceled"), ("Aborted", "aborted"),
    ("CommunicationLost", "needs_attention"), ("Unknown", "needs_attention"),
    ("NeverStarted", "not_run"), ("DidNotRun", "not_run"), ("MissedTheSchedule", "not_run")])
def test_source_failure_families_preserved_without_backups(completion, detail):
    summary, rows = build([job(completionStatus=completion)])[DAY]
    assert summary["failure_count"] == 1
    assert rows[0]["result_detail"] == detail
    assert rows[0]["source_completion_status"] == completion
    assert rows[0]["dashboard_result"] == "failed"
    assert rows[0]["workflow_kind_confirmed"] is False
    assert summary["data_status"] == "partial"


def test_missing_source_status_is_analysis_unconfirmed_not_source_unknown():
    summary, rows = build([job(completionStatus="", state="Completed")])[DAY]
    assert rows[0]["dashboard_result"] == "unconfirmed" and summary["failure_count"] == 0


def test_warning_is_success_subset_and_false_is_not_fabricated():
    jobs = [job(), job(7101, workflowName="SYNTH_SECOND")]
    evidence = {"core:7100": proof(warning=True), "core:7101": proof(warning=False)}
    summary, rows = build(jobs, evidence)[DAY]
    assert summary["success_count"] == 2 and summary["warning_success_count"] == 1
    assert summary["success_rate"] == 1 and summary["data_status"] == "complete"


def test_running_and_direct_save_are_not_workflow_results():
    active = job(state="SessionActive", completionStatus="", endTime=None, exitCode=None)
    summary, rows = build([active, job(9990, type="save job", adhocJob=True)])[DAY]
    assert summary["workflow_count"] == 1 and rows[0]["dashboard_result"] == "running"
    assert summary["success_rate"] is None and summary["excluded_workflow_count"] == 1


def test_next_round_running_or_confirmed_skip_does_not_remove_previous_success():
    later = job(7101, startTime="2026-11-12T00:00:00Z", endTime=None, state="Active", completionStatus="", exitCode=None)
    summary, rows = build([job(), later], {"core:7100": proof()})[DAY]
    assert summary["success_count"] == 1 and rows[0]["workflow_job_id"] == "7100"
    later.update(startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:01:00Z",
                 state="Completed", completionStatus="Succeeded", exitCode=0, message=skip())
    evidence = {"core:7100": proof(), "core:7101": proof(start_time=later["startTime"], target_actions=["backup"], attempted=False)}
    summary, rows = build([job(), later], evidence)[DAY]
    assert summary["success_count"] == 1 and rows[0]["workflow_job_id"] == "7100"
    assert rows[0]["excluded_execution_count"] == 1
    assert rows[0]["execution_results"][1]["result_detail"] == "skipped"


def test_success_then_cancel_after_cutoff_updates_original_day_only_with_evidence():
    late = job(7101, startTime="2026-11-11T22:50:00Z", endTime="2026-11-11T23:05:00Z", completionStatus="Canceled", exitCode=1)
    evidence = {"core:7100": proof(), "core:7101": proof(start_time=late["startTime"], backup_day=DAY)}
    summary, rows = build([job(), late], evidence)[DAY]
    assert summary["failure_count"] == 1 and summary["success_count"] == 0
    assert rows[0]["workflow_job_id"] == "7101" and rows[0]["attribution"] == "execution_evidence"


def test_long_completion_rebuilds_old_day_and_unknown_day_is_never_guessed():
    late = job(startTime="2026-11-11T14:00:00Z", endTime="2026-11-12T13:00:00Z")
    now = datetime(2026, 11, 13, 0, 10, tzinfo=timezone.utc)
    evidence = {"core:7100": proof(start_time=late["startTime"], backup_day=DAY)}
    output = build([late], evidence, now=now)
    assert output[DAY][0]["success_count"] == 1
    assert output["2026-11-12"][0]["success_count"] is None
    output = build([late], {"core:7100": proof(start_time=late["startTime"])}, now=now)
    row = output["2026-11-12"][1][0]
    assert row["backup_day"] is None and row["attribution"] == "unresolved"
    assert row["dashboard_result"] == "unconfirmed"


def test_08_boundary_timezone_and_latest_closed_day():
    start, end = window(DAY)
    assert start.isoformat() == "2026-11-10T23:00:00+00:00"
    assert end.isoformat() == "2026-11-11T23:00:00+00:00"
    assert bucket(end) == "2026-11-12"
    assert closed_days(datetime(2026, 11, 12, 22, 59, tzinfo=timezone.utc), "core", config()) == [DAY]
    assert closed_days(datetime(2026, 11, 12, 23, 0, tzinfo=timezone.utc), "core", config()) == ["2026-11-12"]


def test_empty_unattested_population_is_not_reported_as_confirmed_zero():
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    summary, rows = build_daily([], "core", [DAY], NOW, cfg)[DAY]
    assert rows == [] and summary["success_count"] is None and summary["failure_count"] is None
    assert summary["success_rate"] is None and summary["data_status"] == "partial"


def test_source_separation_reobservation_and_domain_duplicates_do_not_add_counts():
    initial = hits([job()])
    duplicate = deepcopy(initial[0])
    duplicate["_index"] = "nw-unmapped-job-run-2026-11"
    active = hits([job(state="Active", completionStatus="", endTime=None)], collected=NOW)[0]
    cfg = config({"core:7100": proof()})
    a = build_daily(initial + [duplicate, active] + hits([job()], "chnl"), "core", [DAY], NOW, cfg)
    assert a[DAY][0]["success_count"] == 1
    assert a[DAY][1][0]["observed_execution_count"] == 1
    assert a == build_daily(initial + [duplicate, active], "core", [DAY], NOW, cfg)


def test_id_reuse_conflicts_and_tied_final_executions_are_unconfirmed():
    cfg = config({"core:7100": proof()})
    reused = hits([job(startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:20:00Z")])
    summary, rows = build_daily(hits([job()]) + reused, "core", [DAY], NOW, cfg)[DAY]
    assert rows[0]["dashboard_result"] == "unconfirmed" and summary["success_count"] == 0
    summary, rows = build([job(), job(7101)], {"core:7100": proof(), "core:7101": proof()})[DAY]
    assert rows[0]["dashboard_result"] == "unconfirmed"


def test_clone_requires_execution_bound_proof_and_skip_suffix_is_not_proof():
    summary, rows = build([job()], {"core:7100": proof(workflow_kind="clone")})[DAY]
    assert rows[0]["result_detail"] == "clone" and summary["excluded_workflow_count"] == 1
    summary, rows = build([job()], {"core:7100": proof(start_time="2026-11-10T12:00:00Z", workflow_kind="clone")})[DAY]
    assert rows[0]["dashboard_result"] == "success"
    summary, rows = build(hierarchy(), {"core:7100": proof(workflow_kind="clone")})[DAY]
    assert rows[0]["result_detail"] == "mixed_backup_clone"
    assert summary["excluded_workflow_count"] == 0


def test_summary_matches_exact_detail_result_set():
    jobs = [job(), job(7101, workflowName="SYNTH_FAILURE", completionStatus="Failed"),
            job(7102, workflowName="SYNTH_PENDING", completionStatus=""), job(7103, workflowName="SYNTH_SKIP", message=skip())]
    evidence = {"core:7100": proof(), "core:7103": proof(target_actions=["backup"], attempted=False)}
    summary, rows = build(jobs, evidence)[DAY]
    for result_name, count in (("success", "success_count"), ("failed", "failure_count"), ("unconfirmed", "unconfirmed_workflow_count")):
        assert summary[count] == sum(r["dashboard_result"] == result_name for r in rows)
    assert summary["success_rate"] == .5 and {r["generation"] for r in rows} == {summary["generation"]}


def es_error(cls, status):
    meta = ApiResponseMeta(status=status, http_version="1.1", headers={}, duration=0, node=NodeConfig("http", "localhost", 9200))
    return cls("SYNTHETIC", meta, {})


def field(document, name):
    for part in name.split("."):
        document = document.get(part) if isinstance(document, dict) else None
    return document


def matches(document, query):
    if "term" in query:
        return all(str(field(document, k)) == str(v) for k, v in query["term"].items())
    if "terms" in query:
        return all(str(field(document, k)) in [str(v) for v in values] for k, values in query["terms"].items())
    if "exists" in query:
        return field(document, query["exists"]["field"]) is not None
    if "range" in query:
        return all(timestamp(field(document, k)) is not None and all(timestamp(field(document, k)) >= timestamp(v) if op == "gte"
                   else timestamp(field(document, k)) < timestamp(v) for op, v in limits.items()) for k, limits in query["range"].items())
    b = query["bool"]
    return (all(matches(document, q) for q in b.get("filter", [])) and
            not any(matches(document, q) for q in b.get("must_not", [])) and
            sum(matches(document, q) for q in b.get("should", [])) >= b.get("minimum_should_match", 0))


class FakeES:
    def __init__(self, initial=()):
        self.store = {(h["_index"], h["_id"]): deepcopy(h["_source"]) for h in initial}
        self.versions = {k: 0 for k in self.store}
        self.requests, self.bulks, self.pits = [], [], {}
        self.partial, self.fail_details, self.conflict_summary = False, False, False

    async def get(self, **args):
        key = args["index"], args["id"]
        if key not in self.store:
            raise es_error(NotFoundError, 404)
        return {"_source": deepcopy(self.store[key]), "_seq_no": self.versions[key], "_primary_term": 1}

    async def field_caps(self, **args):
        self.requests.append(("field_caps", deepcopy(args)))
        return {"fields": {f: {"date" if f.endswith("end_time") else "keyword": {"searchable": True}} for f in args["fields"]}}

    async def open_point_in_time(self, **args):
        self.requests.append(("open", args))
        patterns = args["index"].split(",")
        self.pits["pit"] = [(k, deepcopy(d)) for k, d in sorted(self.store.items())
                            if any(fnmatch.fnmatchcase(k[0], pattern) for pattern in patterns)]
        return {"id": "pit", "_shards": {"failed": 0}}

    async def close_point_in_time(self, **args):
        self.requests.append(("close", args))
        return {"succeeded": True}

    async def search(self, **args):
        self.requests.append(("search", deepcopy(args)))
        all_hits = [{"_index": key[0], "_id": key[1], "_source": d, "sort": [n]}
                    for n, (key, d) in enumerate(self.pits[args["pit"]["id"]]) if matches(d, args["query"])]
        after = args.get("search_after", [-1])[0]
        page = [h for h in all_hits if h["sort"][0] > after][:args["size"]]
        return {"pit_id": "pit", "timed_out": self.partial, "_shards": {"failed": 0},
                "hits": {"total": {"value": len(all_hits), "relation": "eq"}, "hits": deepcopy(page)}}

    async def mget(self, **args):
        self.requests.append(("mget", args))
        return {"docs": [{"_id": identity, "found": (args["index"], identity) in self.store,
                          "_source": deepcopy(self.store.get((args["index"], identity), {}))} for identity in args["ids"]]}

    async def bulk(self, client, actions, **kwargs):
        actions = list(actions)
        self.bulks.append(deepcopy(actions))
        for a in actions:
            key = a["_index"], a["_id"]
            if self.fail_details and key[1].startswith("workflow:"):
                raise RuntimeError("SYNTHETIC detail failure")
            if self.conflict_summary and key[1].startswith("daily:") and key[1].count(":") == 2:
                raise es_error(ConflictError, 409)
            if a["_op_type"] == "create" and key in self.store:
                raise es_error(ConflictError, 409)
            if "if_seq_no" in a and a["if_seq_no"] != self.versions.get(key):
                raise es_error(ConflictError, 409)
            self.store[key] = deepcopy(self.store.get(key, {})) | deepcopy(a.get("doc", a.get("_source", {})))
            self.versions[key] = self.versions.get(key, -1) + 1
        return len(actions), []


@pytest.mark.asyncio
async def test_snapshot_pagination_children_not_clipped_by_completion_and_source_separation():
    es = FakeES(hits(hierarchy()) + hits([job()], "chnl"))
    cfg = config(page_size=1)
    output = await read_jobs(es, cfg, "core", [DAY], [])
    assert len(output) == 3 and all(h["_source"]["source_networker"] == "core" for h in output)
    child_queries = [a["query"] for name, a in es.requests if name == "search" and "should" in a["query"]["bool"]
                     and "terms" in a["query"]["bool"]["should"][0]]
    assert child_queries and "range" not in json.dumps(child_queries)
    assert es.requests[-1][0] == "close"


@pytest.mark.asyncio
async def test_publish_rebuilds_whole_day_and_is_idempotent_across_restart():
    es = FakeES(hits([job(7101, workflowName="SYNTH_OTHER", completionStatus="Failed")]))
    cfg = config({"core:7100": proof()})
    publisher = NetworkerDailyCurrent(es, es.bulk, cfg)
    await publisher.publish("core", hits([job()]), NOW, True)
    first = deepcopy(es.store)
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", hits([job()]), NOW, True)
    assert first == es.store
    summary = es.store[("nw-current-core", "daily:core:" + DAY)]
    rows = [d for (i, _), d in es.store.items() if i == "nw-current-core" and d.get("document_kind") == "workflow_result"
            and d["generation"] == summary["generation"]]
    assert summary["success_count"] == summary["failure_count"] == 1
    assert len(rows) == 2
    assert all(a["_index"].startswith("nw-current-") for bulk in es.bulks for a in bulk)


@pytest.mark.asyncio
async def test_partial_snapshot_preserves_previous_generation_and_never_publishes_zero():
    es = FakeES(hits([job()]))
    publisher = NetworkerDailyCurrent(es, es.bulk, config({"core:7100": proof()}))
    await publisher.publish("core", [], NOW, True)
    key = "nw-current-core", "daily:core:" + DAY
    before = deepcopy(es.store[key])
    es.partial = True
    with pytest.raises(CurrentReadError):
        await publisher.publish("core", [], NOW, True)
    assert es.store[key]["generation"] == before["generation"]
    assert es.store[key]["success_count"] == 1 and es.store[key]["data_status"] == "unavailable"
    assert es.requests[-1][0] == "close"


@pytest.mark.asyncio
async def test_read_budget_exhaustion_does_not_claim_an_empty_day():
    es = FakeES(hits(hierarchy()))
    with pytest.raises(CurrentReadError):
        await NetworkerDailyCurrent(es, es.bulk, config(max_hits=1)).publish("core", [], NOW, True)
    summary = es.store[("nw-current-core", "daily:core:" + DAY)]
    assert summary["success_count"] is None and summary["generation"] is None


@pytest.mark.asyncio
async def test_failed_detail_or_cas_does_not_switch_summary():
    es = FakeES(hits([job()]))
    cfg = config({"core:7100": proof()})
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    key = "nw-current-core", "daily:core:" + DAY
    previous = deepcopy(es.store[key])
    es.fail_details = True
    with pytest.raises(RuntimeError):
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    assert es.store[key] == previous
    es.fail_details, es.conflict_summary = False, True
    with pytest.raises(ConflictError):
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    assert es.store[key] == previous


@pytest.mark.asyncio
async def test_writer_actual_write_path_generates_current_after_original_bulk(monkeypatch):
    import apps.writers.elasticsearch as module
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr(module, "datetime", Clock)
    es = FakeES()
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=NetworkerCurrentConfig(enabled=True, rebuild_days=1)))
    writer.client, writer._async_bulk = es, es.bulk
    await writer.write_many([result([job()])])
    assert any(a["_index"].startswith("nw-ops-raw-") for a in es.bulks[0])
    assert any(a["_index"] == "nw-core-job-run-2026-11" for a in es.bulks[0])
    assert es.store[("nw-current-core", "daily:core:" + DAY)]["success_count"] == 1
    assert es.bulks[-1][0]["_id"] == "daily:core:" + DAY


def test_kubernetes_configs_and_mapping_cover_current_without_lookup_mode(tmp_path, monkeypatch):
    # Validate the files actually mounted as APP_CONFIG; never read real Secret files.
    monkeypatch.setattr("apps.core.config._optional_secret", lambda raw, key: None)
    for source in ("core", "chnl", "info"):
        documents = list(yaml.safe_load_all(Path(f"k8s/networker-{source}.yaml").read_text(encoding="utf-8-sig")))
        config_map = next(d for d in documents if d["kind"] == "ConfigMap")
        embedded = tmp_path / f"collector-{source}.yaml"
        embedded.write_text(config_map["data"]["collector.yaml"], encoding="utf-8")
        loaded = load_config(embedded)
        current = loaded.elasticsearch.networker_current
        assert current.enabled is False and current.rebuild_days == 7
        assert current.page_size == 500 and current.max_hits == 200000
        assert current.include_unmapped is True
        assert current.workflow_rules == [] and current.run_evidence == {} and current.coverage == {}
        assert loaded.collectors[0].source_networker == source
    template = json.loads(Path("config/elasticsearch_networker_current_index_template.example.json").read_text())
    assert template["index_patterns"] == ["nw-current-core", "nw-current-chnl", "nw-current-info"]
    assert "settings" not in template["template"]
    fields = template["template"]["mappings"]["properties"]
    assert fields["summary_role"] == {"type": "keyword"}
    summary, rows = build(hierarchy())[DAY]
    assert set(summary) <= set(fields) and all(set(row) <= set(fields) for row in rows)


@pytest.mark.parametrize("values", [{"enabled": "false"}, {"rebuild_days": 0}, {"page_size": 1001},
                                  {"max_hits": False}, {"run_evidence": {1: {}}}, {"coverage": {"core:" + DAY: True}}])
def test_invalid_current_settings_cannot_silently_activate_or_attest(values):
    with pytest.raises(ValueError):
        NetworkerCurrentConfig(**values)


@pytest.mark.asyncio
async def test_failed_original_bulk_never_publishes_current():
    es = FakeES()
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=config()))
    writer.client = es
    async def failing_bulk(*args, **kwargs):
        raise RuntimeError("SYNTHETIC source write failure")
    writer._async_bulk = failing_bulk
    await writer.write_many([result([job()])])
    assert es.requests == [] and es.store == {}


@pytest.mark.asyncio
async def test_raw_legacy_message_hydration_is_by_exact_id_and_never_mutates_source():
    from apps.writers.networker_current import hydrate_messages
    original = result([job(message=skip())])
    writer = ElasticsearchWriter(ElasticsearchConfig())
    actions = writer._actions_for_result(original)
    raw = next(a for a in actions if a["_source"].get("document_family") == "raw")
    workflow = next(deepcopy(a) for a in actions if a["_source"].get("document_type") == "job-run")
    workflow["_source"]["payload"].pop("message")
    es = FakeES([raw, workflow])
    before = deepcopy(es.store)
    await hydrate_messages(es, [workflow], "core")
    assert workflow["_source"]["payload"]["message"] == skip()
    assert es.store == before
    request = es.requests[-1]
    assert request[0] == "mget" and request[1]["ids"] == [raw["_id"]]
    assert "query" not in request[1]


@pytest.mark.asyncio
async def test_raw_source_mismatch_cannot_supply_skip_evidence():
    from apps.writers.networker_current import hydrate_messages
    writer = ElasticsearchWriter(ElasticsearchConfig())
    actions = writer._actions_for_result(result([job(message=skip())]))
    raw = next(a for a in actions if a["_source"].get("document_family") == "raw")
    raw["_source"]["payload"]["summary"]["source_networker"] = "chnl"
    workflow = next(deepcopy(a) for a in actions if a["_source"].get("document_type") == "job-run")
    workflow["_source"]["payload"].pop("message")
    await hydrate_messages(FakeES([raw]), [workflow], "core")
    assert "message" not in workflow["_source"]["payload"]


@pytest.mark.asyncio
async def test_published_terminal_result_survives_source_regression_after_restart():
    es = FakeES(hits([job()]))
    cfg = config({"core:7100": proof()})
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    summary_key = "nw-current-core", "daily:core:" + DAY
    original = deepcopy(es.store[summary_key])
    active = hits([job(state="SessionActive", completionStatus="", endTime=None, exitCode=None)])[0]
    es.store[(active["_index"], active["_id"])] = active["_source"]
    with pytest.raises(CurrentReadError, match="terminal_observation_regression"):
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    assert es.store[summary_key]["success_count"] == 1
    assert es.store[summary_key]["generation"] == original["generation"]
    assert es.store[summary_key]["data_status"] == "unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["core", "chnl", "info"])
async def test_three_source_indices_and_stable_summary_identity(source):
    es = FakeES(hits([job()], source))
    cfg = config({source + ":7100": proof()})
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish(source, [], NOW, True)
    assert es.store[("nw-current-" + source, "daily:" + source + ":" + DAY)]["success_count"] == 1


@pytest.mark.asyncio
async def test_pit_is_a_snapshot_and_earlier_source_observation_is_rejected():
    es = FakeES(hits([job()]))
    async with CurrentSnapshot(es, config(), "core") as scan:
        es.store.update({(h["_index"], h["_id"]): h["_source"] for h in hits([job(7101, workflowName="SYNTH_SECOND")])})
        rows = await scan.read({"bool": {"filter": []}})
        assert len(rows) == 1
    cfg = config({"core:7100": proof(), "core:7101": proof()})
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    # An older observation must never replace a published source observation.
    older = hits([job()], collected=datetime(2026, 11, 11, 12, 30, tzinfo=timezone.utc))[0]
    es.store[(older["_index"], older["_id"])] = older["_source"]
    with pytest.raises(CurrentReadError, match="older_source_observation"):
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)


def test_normal_operation_needs_no_run_evidence_no_children_and_no_coverage_attestation():
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    summary, rows = build_daily(hits([job()]), "core", [DAY], NOW, cfg)[DAY]
    assert cfg.run_evidence == {} and cfg.workflow_rules == ()
    assert rows[0]["dashboard_result"] == "success"
    assert summary["success_count"] == 1 and summary["failure_count"] == 0 and summary["success_rate"] == 1
    assert summary["unconfirmed_workflow_count"] == 0 and summary["warning_success_count"] is None
    assert summary["aggregation_status"] == "ok" and summary["coverage_status"] == "unverified"
    assert summary["data_status"] == "partial"
    assert "actual_execution_unconfirmed" not in rows[0]["analysis_reasons"]


def test_unsupported_auxiliary_type_does_not_invalidate_succeeded_workflow():
    jobs = [job(), job(7200, type="synthetic unsupported auxiliary", rootParentJobId=7100)]
    summary, rows = build(jobs)[DAY]
    assert summary["success_count"] == 1 and summary["unconfirmed_workflow_count"] == 0
    assert rows[0]["dashboard_result"] == "success"
    assert "unsupported_auxiliary_job_not_used_for_outcome" in rows[0]["analysis_reasons"]


@pytest.mark.parametrize("completion,detail", [("NeverStarted", "not_run"), ("DidNotRun", "not_run"),
    ("MissedTheSchedule", "not_run"), ("Canceled", "canceled"), ("Aborted", "aborted"),
    ("Abandoned", "aborted"), ("CommunicationLost", "needs_attention"), ("Unknown", "needs_attention")])
def test_known_day_terminal_failure_without_end_is_counted_with_null_duration(completion, detail):
    jobs = [job(completionStatus=completion, endTime=None)]
    settings = [rule(backup_day_basis="start_window", workflow_kind="backup")]
    summary, rows = build(jobs, rules=settings)[DAY]
    assert rows[0]["backup_day"] == DAY and rows[0]["attribution"] == "workflow_start_rule"
    assert rows[0]["dashboard_result"] == "failed" and rows[0]["result_detail"] == detail
    assert rows[0]["source_completion_status"] == completion
    assert rows[0]["end_time"] is None and rows[0]["duration_seconds"] is None
    assert summary["failure_count"] == 1 and summary["success_count"] == 0
    assert summary["duration_seconds_sum"] is None and summary["duration_seconds_average"] is None
    assert summary["duration_sample_count"] == 0 and summary["aggregation_status"] == "ok"


def test_no_end_without_known_backup_day_is_not_arbitrarily_assigned():
    summary, rows = build([job(completionStatus="Abandoned", endTime=None)])[DAY]
    assert rows[0]["backup_day"] is None and rows[0]["dashboard_result"] == "unconfirmed"
    assert summary["failure_count"] == 0 and summary["unconfirmed_workflow_count"] == 1


def test_source_success_does_not_depend_on_manual_attempted_flag_or_end_time_when_day_known():
    summary, rows = build([job()], {"core:7100": proof(attempted=False)})[DAY]
    assert summary["success_count"] == 1 and rows[0]["dashboard_result"] == "success"
    summary, rows = build([job(endTime=None)], rules=[rule(backup_day_basis="start_window")])[DAY]
    assert summary["success_count"] == 1 and rows[0]["dashboard_result"] == "success"
    assert summary["last_actual_success_at"] is None
    assert summary["duration_sample_count"] == 0 and summary["duration_seconds_average"] is None


def test_abandoned_state_without_completion_is_a_terminal_failure_when_day_is_known():
    summary, rows = build([job(completionStatus="", state="Abandoned", endTime=None)],
                         rules=[rule(backup_day_basis="start_window")])[DAY]
    assert rows[0]["dashboard_result"] == "failed" and rows[0]["result_detail"] == "aborted"
    assert summary["failure_count"] == 1


def test_explicit_not_run_with_backup_skip_and_known_day_is_excluded():
    jobs = [job(completionStatus="DidNotRun", endTime=None, message=skip())]
    summary, rows = build(jobs, rules=[rule(backup_day_basis="start_window")])[DAY]
    assert summary["failure_count"] == 0 and rows[0]["result_detail"] == "skipped"


def test_reusable_exact_workflow_rules_handle_clone_and_skip_without_run_ids():
    for identity in (7100, 7101):
        summary, rows = build([job(identity)], rules=[rule(workflow_kind="clone")])[DAY]
        assert rows[0]["result_detail"] == "clone" and summary["excluded_workflow_count"] == 1
        summary, rows = build([job(identity, message=skip())], rules=[rule(target_actions=["backup"])])[DAY]
        assert rows[0]["result_detail"] == "skipped" and summary["excluded_workflow_count"] == 1
    summary, rows = build([job(workflowName="SYNTH_OTHER")], rules=[rule(workflow_kind="clone")])[DAY]
    assert rows[0]["dashboard_result"] == "success"  # Name similarity is not a rule match.
    summary, rows = build([job()], rules=[rule(workflow_kind="clone", effective_from="2026-11-12T00:00:00Z")])[DAY]
    assert rows[0]["dashboard_result"] == "success"  # Current settings don't rewrite older executions.


def test_rule_validity_and_overlaps_are_not_silently_guessed():
    with pytest.raises(ValueError):
        config(workflow_rules=[rule(effective_from="2026-11-01T00:00:00")])
    summary, rows = build([job()], rules=[rule(workflow_kind="backup"), rule(workflow_kind="clone")])[DAY]
    assert rows[0]["dashboard_result"] == "unconfirmed"
    assert "overlapping_workflow_operating_rules" in rows[0]["analysis_reasons"]


@pytest.mark.parametrize("value", [DAY, None, ""])
def test_configuration_loading_rejects_fixed_day_in_repeated_rule(tmp_path, value):
    settings = {"elasticsearch": {"networker_current": {"workflow_rules": [
        rule(), rule(workflow_name="SYNTH_OTHER", backup_day=value)]}}}
    path = tmp_path / "collector.yaml"
    path.write_text(yaml.safe_dump(settings), encoding="utf-8")
    with pytest.raises(ValueError) as error:
        load_config(path)
    assert "workflow_rules[1].backup_day" in str(error.value)
    assert "backup_day는 run_evidence의 실행별 예외에서만 사용할 수 있음" in str(error.value)


def test_configuration_loading_accepts_valid_execution_day_exception(tmp_path):
    late = job(startTime="2026-11-11T14:00:00Z", endTime="2026-11-12T13:00:00Z")
    evidence = {"core:7100": proof(start_time=late["startTime"], backup_day=DAY)}
    path = tmp_path / "collector.yaml"
    path.write_text(yaml.safe_dump({"elasticsearch": {"networker_current": {
        "rebuild_days": 1, "run_evidence": evidence}}}), encoding="utf-8")
    cfg = load_config(path).elasticsearch.networker_current
    now = datetime(2026, 11, 13, 0, 10, tzinfo=timezone.utc)
    summary, rows = build_daily(hits([late]), "core", closed_days(now, "core", cfg), now, cfg)[DAY]
    assert rows[0]["backup_day"] == DAY and rows[0]["attribution"] == "execution_evidence"
    assert summary["success_count"] == 1


@pytest.mark.parametrize("invalid", ["start", "reference", "identity"])
def test_execution_day_exception_requires_matching_execution_and_reference(invalid):
    late = job(startTime="2026-11-11T14:00:00Z", endTime="2026-11-12T13:00:00Z")
    now = datetime(2026, 11, 13, 0, 10, tzinfo=timezone.utc)
    evidence = {"core:7100": proof(start_time=late["startTime"], backup_day=DAY)}
    if invalid == "start":
        evidence["core:7100"]["start_time"] = START
    elif invalid == "reference":
        evidence["core:7100"]["reference"] = ""
    else:
        evidence = {"chnl:7100": evidence["core:7100"]}
    output = build([late], evidence, now=now)
    row = output["2026-11-12"][1][0]
    assert row["dashboard_result"] == "unconfirmed" and row["backup_day"] is None


@pytest.mark.asyncio
async def test_reusable_start_day_rule_discovers_and_rebuilds_complete_older_day():
    late = job(startTime="2026-11-11T14:00:00Z", endTime="2026-11-12T13:00:00Z")
    older = job(7101, workflowName="SYNTH_OTHER", completionStatus="Failed")
    now = datetime(2026, 11, 13, 0, 10, tzinfo=timezone.utc)
    es = FakeES(hits([late, older]))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1,
        workflow_rules=[rule(workflow_kind="backup", backup_day_basis="start_window")])
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], now, True)
    summary = es.store[("nw-current-core", "daily:core:" + DAY)]
    assert summary["success_count"] == 1 and summary["failure_count"] == 1
    assert len([r for r in es.requests if r[0] == "open"]) == 2
    assert cfg.run_evidence == {}


@pytest.mark.asyncio
async def test_no_end_failure_is_idempotent_after_restart():
    es = FakeES(hits([job(completionStatus="Abandoned", endTime=None)]))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1,
                                workflow_rules=[rule(backup_day_basis="start_window")])
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    initial = deepcopy(es.store)
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    assert es.store == initial
    assert es.store[("nw-current-core", "daily:core:" + DAY)]["failure_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("include_unmapped", [True, False])
async def test_core_reader_needs_no_chnl_or_info_index_privileges(include_unmapped):
    class CoreOnlyES(FakeES):
        async def field_caps(self, **args):
            assert "chnl" not in args["index"] and "info" not in args["index"]
            return await super().field_caps(**args)
        async def open_point_in_time(self, **args):
            assert "chnl" not in args["index"] and "info" not in args["index"]
            return await super().open_point_in_time(**args)
    core = hits([job()])[0]
    copy = deepcopy(core)
    copy["_index"] = "nw-unmapped-job-run-2026-11"
    other_source_unmapped = hits([job(7990)], "chnl")[0]
    other_source_unmapped["_index"] = "nw-unmapped-job-run-2026-11"
    es = CoreOnlyES([core, copy, other_source_unmapped] + hits([job()], "chnl") + hits([job()], "info"))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1, include_unmapped=include_unmapped)
    found = await read_jobs(es, cfg, "core", [DAY], [])
    assert len(found) == (2 if include_unmapped else 1)
    assert all(h["_source"]["source_networker"] == "core" for h in found)


def test_grafana_native_variable_fragment_fetches_generation_instead_of_pinning_it():
    fragment = json.loads(Path("config/grafana_networker_current_variables.example.json").read_text(encoding="utf-8"))
    variables = {v["name"]: v for v in fragment["templating"]["list"]}
    source, generation = variables["nw_source"], variables["nw_generation"]
    assert source["query"] == "core,chnl,info" and not source["multi"] and not source["includeAll"]
    query = json.loads(generation["query"])
    assert query["find"] == "terms" and query["field"] == "generation"
    assert "document_kind:daily_summary" in query["query"]
    assert "summary_role:head" in query["query"]
    assert "${nw_source:lucene}" in query["query"]
    assert "window_end:{now-24h TO now]" in query["query"]
    assert generation["current"] == {} and not generation["multi"] and not generation["includeAll"]
    assert generation["refresh"] == 2 and generation["skipUrlSync"] is True
    assert fragment["time"]["to"] == "now"


def test_sample_skip_message_excludes_with_empty_rules_and_run_evidence():
    # Same message clauses as the supplied sample, with anonymous identifiers.
    message = ("Starting synthetic:Protection Policy 'SYNTH_POLICY' workflow 'SYNTH_W'.\n"
        "Consider starting action 'backup', enabled 1, schedule action 'skip'\n"
        "Request to run the enabled action 'backup' that has level configured as 'skip'.\n"
        "Not running action 'backup' because either the schedule says to skip it today, or, "
        "the previously failed action was not restarted within the specified restart window.\n"
        "Worklofw 'SYNTH_POLICY/SYNTH_W' succeeded.")
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    assert cfg.run_evidence == {} and cfg.workflow_rules == ()
    summary, rows = build_daily(hits([job(message=message)]), "core", [DAY], NOW, cfg)[DAY]
    assert rows[0]["source_completion_status"] == "Succeeded"
    assert rows[0]["source_status_normalized"] == "success"
    assert rows[0]["dashboard_result"] == "excluded" and rows[0]["result_detail"] == "skipped"
    assert summary["success_count"] == summary["failure_count"] == 0
    assert summary["excluded_workflow_count"] == 1 and summary["success_rate"] is None
    # Names and absent children are insufficient when the explicit message is absent.
    summary, rows = build_daily(hits([job()]), "core", [DAY], NOW, cfg)[DAY]
    assert rows[0]["dashboard_result"] == "success" and summary["excluded_workflow_count"] == 0


def current_generation_documents(es, generation, role):
    return [d for (index, _), d in es.store.items() if index == "nw-current-core"
            and d.get("document_kind") == "daily_summary" and d.get("summary_role") == role
            and d.get("generation") == generation]


@pytest.mark.asyncio
async def test_message_only_skip_publishes_matching_summary_and_detail_without_source_mutation():
    original = hits([job(message=skip())])
    es = FakeES(original)
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    head = es.store[("nw-current-core", "daily:core:" + DAY)]
    snapshot = current_generation_documents(es, head["generation"], "generation")[0]
    detail = es.store[("nw-current-core", head["detail_ids"][0])]
    assert snapshot["success_count"] == snapshot["failure_count"] == 0
    assert snapshot["excluded_workflow_count"] == 1 and snapshot["success_rate"] is None
    assert detail["generation"] == snapshot["generation"]
    assert detail["dashboard_result"] == "excluded" and detail["result_detail"] == "skipped"
    assert detail["source_completion_status"] == "Succeeded" and detail["source_status_normalized"] == "success"
    assert detail["evidence_reference"] is None
    for doc in original:
        assert es.store[(doc["_index"], doc["_id"])] == doc["_source"]


@pytest.mark.asyncio
async def test_selected_a_summary_and_details_remain_readable_after_b_publication():
    es = FakeES(hits([job()]))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    head_id = "daily:core:" + DAY
    selected_a = deepcopy((await es.get(index="nw-current-core", id=head_id))["_source"])
    generation_a = selected_a["generation"]
    snapshot_a = deepcopy((await es.get(index="nw-current-core", id=head_id + ":" + generation_a))["_source"])
    late_failure = hits([job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:10:00Z", completionStatus="Failed")])[0]
    es.store[(late_failure["_index"], late_failure["_id"])] = late_failure["_source"]
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    head_b = (await es.get(index="nw-current-core", id=head_id))["_source"]
    assert head_b["generation"] != generation_a
    assert head_b["success_count"] == 0 and head_b["failure_count"] == 1
    # The fixed-ID head HAS lost A; querying the generation-summary role avoids that loss.
    assert current_generation_documents(es, generation_a, "head") == []
    assert current_generation_documents(es, generation_a, "generation") == [snapshot_a]
    read_a = (await es.get(index="nw-current-core", id=head_id + ":" + generation_a))["_source"]
    assert read_a == snapshot_a and read_a["success_count"] == 1 and read_a["failure_count"] == 0
    details_a = (await es.mget(index="nw-current-core", ids=read_a["detail_ids"]))["docs"]
    assert all(d["found"] and d["_source"]["generation"] == generation_a for d in details_a)
    assert read_a["success_count"] == sum(d["_source"]["dashboard_result"] == "success" for d in details_a)
    assert read_a["failure_count"] == sum(d["_source"]["dashboard_result"] == "failed" for d in details_a)
    assert {d.get("document_kind") for (i, _), d in es.store.items() if i == "nw-current-core"} == {"daily_summary", "workflow_result"}
    assert all(a["_index"] == "nw-current-core" for actions in es.bulks for a in actions)


@pytest.mark.asyncio
async def test_legacy_head_only_a_is_preserved_before_upgrade_overwrites_it():
    es = FakeES(hits([job()]))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    head_key = "nw-current-core", "daily:core:" + DAY
    generation_a = es.store[head_key]["generation"]
    snapshot_key = head_key[0], head_key[1] + ":" + generation_a
    # Fixture representing the prior implementation: only a fixed summary and A details.
    es.store.pop(snapshot_key)
    es.store[head_key].pop("summary_role")
    late = hits([job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:10:00Z", completionStatus="Failed")])[0]
    es.store[(late["_index"], late["_id"])] = late["_source"]
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    assert es.store[snapshot_key]["success_count"] == 1
    assert es.store[snapshot_key]["summary_role"] == "generation"
    assert es.store[head_key]["failure_count"] == 1


@pytest.mark.asyncio
async def test_orphan_b_snapshot_is_not_advertised_and_a_snapshot_is_immutable():
    es = FakeES(hits([job()]))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    head_key = "nw-current-core", "daily:core:" + DAY
    generation_a = es.store[head_key]["generation"]
    snapshot_key = head_key[0], head_key[1] + ":" + generation_a
    original = deepcopy(es.store[snapshot_key])
    # A repeated publication and an availability-only update must not rewrite A's snapshot.
    await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], datetime(2026, 11, 12, 0, 11, tzinfo=timezone.utc), True)
    es.partial = True
    with pytest.raises(CurrentReadError):
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    assert es.store[snapshot_key] == original
    es.partial = False
    late = hits([job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:10:00Z", completionStatus="Failed")])[0]
    es.store[(late["_index"], late["_id"])] = late["_source"]
    es.conflict_summary = True
    with pytest.raises(ConflictError):
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
    generations = {d["generation"] for (i, _), d in es.store.items()
                   if i == "nw-current-core" and d.get("summary_role") == "generation"}
    assert len(generations) == 2  # B is stored, but its CAS head publication failed.
    heads = [d for (i, _), d in es.store.items() if i == "nw-current-core" and d.get("summary_role") == "head"]
    assert {d["generation"] for d in heads} == {generation_a}
    assert es.store[snapshot_key] == original


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [409, 400])
async def test_generation_summary_create_only_tolerates_a_verified_duplicate(status):
    class RaceES(FakeES):
        async def bulk(self, client, actions, **kwargs):
            actions = list(actions)
            for action in actions:
                if action.get("_source", {}).get("summary_role") == "generation":
                    if status == 409:
                        key = action["_index"], action["_id"]
                        self.store[key] = deepcopy(action["_source"])
                        self.versions[key] = 0
                    raise BulkIndexError("SYNTHETIC", [{"create": {"status": status, "_id": action["_id"]}}])
            return await super().bulk(client, actions, **kwargs)
    es = RaceES(hits([job()]))
    cfg = NetworkerCurrentConfig(enabled=True, rebuild_days=1)
    if status == 400:
        with pytest.raises(BulkIndexError):
            await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
        assert ("nw-current-core", "daily:core:" + DAY) not in es.store
    else:
        await NetworkerDailyCurrent(es, es.bulk, cfg).publish("core", [], NOW, True)
        assert es.store[("nw-current-core", "daily:core:" + DAY)]["success_count"] == 1

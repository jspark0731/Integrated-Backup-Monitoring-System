"""Offline CLI contract tests: synthetic ES exports, no live service access."""

from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys

import pytest

from tools import generate_networker_step4b_poc as poc
from tools import networker_action_offline as offline


@pytest.fixture
def observations():
    f = poc.build_fixture()
    jobs = [deepcopy(h) for h in f["job_run_hits"] if h["_source"]["source_networker"] == "core"
            and h["_source"]["payload"]["job_id"] in {"8001", "8101", "8201", "8202"}]
    raw = deepcopy(f["raw_initial_inputs"][0])
    raw["_source"]["payload"]["raw"]["backups"]["backups"] = [
        b for b in raw["_source"]["payload"]["raw"]["backups"]["backups"]
        if b["attributes"][0]["values"] == ["8101"]]
    return [raw], jobs


def export(tmp_path, raw, jobs, *, page_size=2):
    manifest = {"format_version": 1}
    files = {}
    for channel, hits in [("raw", raw), ("jobs", jobs)]:
        folder = tmp_path / channel
        folder.mkdir()
        query_hash = ("a" if channel == "raw" else "b") * 64
        pages, paths, cursor = [], [], None
        chunks = [hits[i:i + page_size] for i in range(0, len(hits), page_size)] + [[]]
        for n, chunk in enumerate(chunks):
            sorted_hits = [deepcopy(hit) | {"sort": [n * page_size + position]} for position, hit in enumerate(chunk)]
            response = {"hits": {"hits": sorted_hits}, "pit_id": channel + "-pit-" + str(n),
                        "timed_out": False, "_shards": {"total": 1, "successful": 1, "failed": 0, "skipped": 0}}
            if n == 0:
                response["hits"]["total"] = {"value": len(hits), "relation": "eq"}
            path = folder / f"page-{n:04}.json"
            path.write_text(json.dumps(response, ensure_ascii=False), encoding="utf-8")
            paths.append(path.resolve())
            pages.append({"file": path.relative_to(tmp_path).as_posix(), "sha256": offline.file_digest(path),
                          "request_pit_id": channel + "-open" if n == 0 else channel + "-pit-" + str(n - 1),
                          "search_after": cursor, "request_query_sha256": query_hash})
            if sorted_hits:
                cursor = sorted_hits[-1]["sort"]
        files[channel] = paths
        manifest[channel] = {"pit_used": True, "query_scope_reviewed": True, "query_sha256": query_hash,
                             "expected_total": len(hits), "indices": sorted({h["_index"] for h in hits}) or ["synthetic-empty"],
                             "pages": pages}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return files, manifest, path


def analyse(files, manifest=None, path=None, **kwargs):
    return offline.analyse(files["raw"], files["jobs"], manifest=manifest,
                           manifest_base=path.parent if path else Path.cwd(), **kwargs)


def mutate_page(files, manifest, channel, n, mutate):
    path = files[channel][n]
    page = json.loads(path.read_text(encoding="utf-8"))
    mutate(page)
    path.write_text(json.dumps(page), encoding="utf-8")
    manifest[channel]["pages"][n]["sha256"] = offline.file_digest(path)


def test_normal_relation_many_backups_and_no_implicit_coverage(tmp_path, observations):
    files, manifest, path = export(tmp_path, *observations)
    summary = analyse(files, manifest, path)
    stats = summary["sources"]["core"]
    assert summary["incomplete"] is False
    assert summary["coverage_supplied"] is False
    assert stats["action_lookup_keys"] == 1
    assert stats["mapping_status"] == {"resolved": 1, "ambiguous": 0, "unresolved": 0, "invalid": 0}
    assert stats["verification_status"]["provisional"] == 1
    assert stats["verification_status"]["verified"] == 0
    assert stats["job_observations_by_type_and_status"]["save job"]["failed"] == 1
    assert all(x["complete_within_declared_es_scope"] for x in summary["es_export"].values())


def test_single_search_size_is_not_proof_of_complete_input(tmp_path, observations):
    files, manifest, path = export(tmp_path, *observations)
    summary = offline.analyse(files["raw"][:1], files["jobs"][:1])
    assert summary["incomplete"] is True
    assert "export_manifest_missing" in summary["es_export"]["raw"]["incomplete_reasons"]
    assert "terminal_empty_page_missing" in summary["es_export"]["jobs"]["incomplete_reasons"]


@pytest.mark.parametrize("fault", ["hash", "cursor", "pit", "query", "total", "index", "scope"])
def test_manifest_consistency_problems_are_incomplete_not_verified(tmp_path, observations, fault):
    files, manifest, path = export(tmp_path, *observations)
    if fault == "hash":
        manifest["raw"]["pages"][0]["sha256"] = "0" * 64
    elif fault == "cursor":
        manifest["jobs"]["pages"][1]["search_after"] = [999]
    elif fault == "pit":
        manifest["jobs"]["pages"][1]["request_pit_id"] = "wrong"
    elif fault == "query":
        manifest["jobs"]["pages"][1]["request_query_sha256"] = "0" * 64
    elif fault == "total":
        manifest["jobs"]["expected_total"] += 1
    elif fault == "index":
        manifest["jobs"]["indices"] = ["wrong"]
    elif fault == "scope":
        manifest["jobs"]["query_scope_reviewed"] = False
    summary = analyse(files, manifest, path)
    assert summary["incomplete"]
    assert sum(s["verification_status"]["verified"] for s in summary["sources"].values()) == 0


def test_missing_page_or_reordered_manifest_is_rejected(tmp_path, observations):
    files, manifest, path = export(tmp_path, *observations)
    with pytest.raises(offline.OfflineError, match="manifest_input_order_mismatch"):
        offline.analyse(files["raw"], files["jobs"][1:], manifest=manifest, manifest_base=path.parent)


@pytest.mark.parametrize("fault", ["timeout", "shard", "partial", "gte", "missing_metadata", "non_increasing"])
def test_partial_response_metadata_marks_incomplete(tmp_path, observations, fault):
    files, manifest, path = export(tmp_path, *observations)

    def mutate(page):
        if fault == "timeout": page["timed_out"] = True
        elif fault == "shard": page["_shards"]["failed"] = 1
        elif fault == "partial": page["is_partial"] = True
        elif fault == "gte": page["hits"]["total"]["relation"] = "gte"
        elif fault == "missing_metadata": del page["_shards"]
        elif fault == "non_increasing": page["hits"]["hits"][1]["sort"] = [0]
    mutate_page(files, manifest, "jobs", 0, mutate)
    assert analyse(files, manifest, path)["incomplete"]


def test_duplicate_physical_export_and_changed_versions_are_detected(tmp_path, observations):
    raw, jobs = observations
    jobs += [deepcopy(jobs[0])]
    jobs[-1]["_source"]["payload"]["status_normalized"] = "running"
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["es_export"]["jobs"]["duplicate_physical_hits"] == 1
    assert summary["es_export"]["jobs"]["physical_version_conflicts"] == 1
    assert summary["incomplete"]


def test_repeated_raw_poll_is_not_a_second_action_candidate(tmp_path, observations):
    raw, jobs = observations
    repeated = deepcopy(raw[0])
    repeated["_id"] += "-second-poll"
    raw.append(repeated)
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["sources"]["core"]["action_lookup_keys"] == 1
    assert summary["sources"]["core"]["mapping_status"]["resolved"] == 1
    assert summary["es_export"]["raw"]["duplicate_physical_hits"] == 0


def test_conflicting_attributes_and_missing_relationships_are_counted(tmp_path):
    fixture = poc.build_fixture()
    files, manifest, path = export(tmp_path, fixture["raw_initial_inputs"], fixture["job_run_hits"])
    summary = analyse(files, manifest, path)
    core, chnl = summary["sources"]["core"], summary["sources"]["chnl"]
    assert core["mapping_status"]["ambiguous"] == 1
    assert core["missing_action_job"] == 3  # 8105, 8998, 8999; no publication omission here.
    assert core["job_root_unconfirmed_observations"]["save job"] == 2
    assert chnl["mapping_status"]["resolved"] == 1
    assert core["job_observations_by_type_and_status"]["save job"]["failed"] == 3
    assert core["selected_action_status"]["failed"] == 1
    assert summary["incomplete"]


@pytest.mark.parametrize("missing", ["root", "workflow", "action", "attributes", "partial_raw"])
def test_missing_and_partial_evidence_remains_visible(tmp_path, observations, missing):
    raw, jobs = observations
    if missing == "root":
        jobs[1]["_source"]["payload"]["root_parent_job_id"] = 0
    elif missing == "workflow":
        jobs = jobs[1:]
    elif missing == "action":
        jobs = [j for j in jobs if j["_source"]["payload"]["type"] != "backup action job"]
    elif missing == "attributes":
        raw[0]["_source"]["payload"]["raw"]["backups"]["backups"][0]["attributes"] = []
    else:
        raw[0]["_source"]["payload"]["collection_status"] = "partial"
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["incomplete"]
    assert summary["sources"]["core"]["verification_status"]["verified"] == 0
    if missing == "root": assert summary["sources"]["core"]["workflow_root_unconfirmed"] == 1
    if missing == "workflow": assert summary["sources"]["core"]["workflow_job_missing"] == 1
    if missing == "action": assert summary["sources"]["core"]["missing_action_job"] == 1


def test_id_reuse_is_blocked_and_original_failure_observation_preserved(tmp_path, observations):
    raw, jobs = observations
    reused = deepcopy(jobs[1])
    reused["_index"] = "synthetic-other-month"
    reused["_source"]["payload"]["start_time"] = "2026-10-01T00:00:00+00:00"
    jobs.append(reused)
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["sources"]["core"]["verification_status"]["blocked"] == 1
    assert summary["sources"]["core"]["job_observations_by_type_and_status"]["save job"]["failed"] == 1


def test_month_boundary_and_source_separation_use_values_not_index_guessing(tmp_path):
    f = poc.build_fixture()
    raw = deepcopy(f["raw_initial_inputs"])
    jobs = [deepcopy(h) for h in f["job_run_hits"] if h["_source"]["payload"]["job_id"] in {"8001", "8101", "8201", "8202"}]
    for hit in jobs:
        hit["_index"] = "synthetic-job-start-october"
        p = hit["_source"]["payload"]
        p["start_time"] = "2026-10-31T23:59:00Z"
        p["end_time"] = "2026-11-06T00:21:00Z"
    for hit in raw:
        hit["_index"] = "synthetic-raw-collected-november"
        hit["_source"]["payload"]["raw"]["backups"]["backups"] = [
            b for b in hit["_source"]["payload"]["raw"]["backups"]["backups"] if b["attributes"][0]["values"] == ["8101"]]
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["sources"]["core"]["mapping_status"]["resolved"] == 1
    assert summary["sources"]["chnl"]["mapping_status"]["resolved"] == 1
    assert summary["sources"]["info"]["action_lookup_keys"] == 0


def test_streaming_parser_handles_tiny_chunks_metadata_after_hits_and_big_integers():
    hit = {"_id": "x", "_index": "y", "_source": {"large": 4294967295, "text": "한국어"}}
    text = json.dumps({"hits": {"hits": [hit], "total": {"relation": "eq", "value": 1}}, "timed_out": False})
    metadata = {}
    reader = offline.JsonStream(io.StringIO(text), chunk_size=1)
    assert list(reader.search_hits(metadata)) == [hit]
    assert metadata["hits.total"]["value"] == 1


def test_parser_is_hit_streaming_not_whole_file_loading():
    # Total file is larger than the per-value bound; each hit is small.
    text = json.dumps({"hits": {"hits": [{"_source": {"v": "x" * 100}} for _ in range(200)]}})
    assert len(text) > 10000
    reader = offline.JsonStream(io.StringIO(text), chunk_size=128, max_value_chars=512)
    assert len(list(reader.search_hits({}))) == 200


@pytest.mark.parametrize("text", ['{"hits":{"hits":[]},"hits":{"hits":[]}}',
                                  '{"hits":{"hits":[{"x":1,"x":2}]}}', '{"hits":{"hits":[',
                                  '{"hits":{"hits":[]}} trailing', '{"hits":{"hits":[NaN]}}'])
def test_invalid_json_never_produces_a_complete_report(text):
    with pytest.raises(offline.OfflineError):
        list(offline.JsonStream(io.StringIO(text)).search_hits({}))


def test_large_value_and_total_record_limit_fail_closed(tmp_path, observations):
    reader = offline.JsonStream(io.StringIO('{"hits":{"hits":[{"x":"' + 'a' * 1000 + '"}]}}'),
                                chunk_size=64, max_value_chars=100)
    with pytest.raises(offline.OfflineError, match="value_limit"):
        list(reader.search_hits({}))
    files, manifest, path = export(tmp_path, *observations)
    with pytest.raises(offline.OfflineError, match="record_limit"):
        analyse(files, manifest, path, max_records=1)


def test_cli_standard_library_only_no_network_and_private_output(tmp_path, observations):
    raw, jobs = observations
    sentinel = "PRIVATE_HOST_CLIENT_PASSWORD_ATTRIBUTE_987"
    raw[0]["_source"]["payload"]["raw"]["backups"]["backups"][0]["attributes"].append(
        {"key": "private", "values": [sentinel]})
    jobs[0]["_source"]["collector"] = sentinel
    files, manifest, manifest_path = export(tmp_path, raw, jobs)
    output = tmp_path / "output"
    wrapper = """import socket, runpy, sys
def deny(*args, **kwargs): raise AssertionError('NETWORK_FORBIDDEN')
socket.socket.connect = deny
socket.create_connection = deny
socket.getaddrinfo = deny
module = runpy.run_path(sys.argv[1], run_name='offline_test')
result = module['main'](sys.argv[2:])
assert not any(n in sys.modules for n in ['elasticsearch', 'httpx', 'requests', 'pydantic'])
raise SystemExit(result)
"""
    result = subprocess.run([sys.executable, "-S", "-c", wrapper, str(Path(offline.__file__)),
                             "--raw", str(tmp_path / "raw"), "--jobs", str(tmp_path / "jobs"),
                             "--manifest", str(manifest_path), "--output", str(output)],
                            capture_output=True, text=True, encoding="utf-8")
    assert result.returncode == 0, result.stderr
    texts = result.stdout + result.stderr + (output / "summary.json").read_text(encoding="utf-8") + (output / "summary.md").read_text(encoding="utf-8")
    assert sentinel not in texts
    for secret in ["synthetic-client", "POC_CORE_POLICY", "555001", "8101", "raw-pit", str(tmp_path)]:
        assert secret not in texts
    assert json.loads(result.stdout)["sources"]["core"]["mapping_status"]["resolved"] == 1


def test_incomplete_cli_exit_three_and_existing_output_preserved(tmp_path, observations, capsys):
    files, manifest, path = export(tmp_path, *observations)
    arguments = ["--raw", str(files["raw"][0]), "--jobs", str(files["jobs"][0]), "--output", str(tmp_path / "out")]
    assert offline.main(arguments) == 3
    before = (tmp_path / "out/summary.json").read_bytes()
    assert offline.main(arguments) == 2
    assert (tmp_path / "out/summary.json").read_bytes() == before
    assert "output_exists" in capsys.readouterr().err


@pytest.mark.parametrize("data", [b"\x9b DRMONE encrypted", b"\x00binary", b"not-json-SECRET_SERVER"])
def test_protected_or_invalid_input_has_sanitized_error_and_no_output(tmp_path, data, capsys):
    raw = tmp_path / "SECRET_SERVER.json"
    jobs = tmp_path / "jobs.json"
    raw.write_bytes(data)
    jobs.write_text('{"hits":{"hits":[]}}', encoding="utf-8")
    assert offline.main(["--raw", str(raw), "--jobs", str(jobs), "--output", str(tmp_path / "out")]) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and "SECRET_SERVER" not in captured.err
    assert not (tmp_path / "out").exists()


def test_invalid_source_classifier_message_is_never_echoed(tmp_path, observations, capsys):
    raw, jobs = observations
    raw[0]["_source"]["source_networker"] = "CREDENTIAL_SECRET"
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert "CREDENTIAL_SECRET" not in json.dumps(summary)
    assert summary["unattributed_issue_counts"]["invalid_source"] == 1


def test_bad_cli_arguments_do_not_echo_sensitive_values(capsys):
    assert offline.main(["--password", "SECRET_CREDENTIAL"]) == 2
    captured = capsys.readouterr()
    assert "SECRET_CREDENTIAL" not in captured.err and captured.out == ""


def test_queries_are_read_only_except_ephemeral_pit_contexts():
    path = Path("docs/networker_job_mapping_step4c1_queries.http")
    requests = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(("GET ", "POST ", "DELETE ", "PUT ")):
            method, endpoint = line.split(" ", 1)
            requests.append((method, endpoint))
            assert method == "GET" or method == "POST" and ("/_pit?" in endpoint or endpoint.startswith("_search?")) or method == "DELETE" and endpoint == "_pit"
            assert not any(s in endpoint for s in ["_bulk", "_doc", "_update", "_reindex", "_index_template"])
    assert len(requests) == 14


def test_invalid_evidence_with_a_known_action_has_invalid_blocked_counts(tmp_path, observations):
    raw, jobs = observations
    for backup in raw[0]["_source"]["payload"]["raw"]["backups"]["backups"]:
        backup["attributes"] = [a for a in backup["attributes"] if a["key"] != "group"]
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["sources"]["core"]["mapping_status"]["invalid"] == 1
    assert summary["sources"]["core"]["verification_status"]["blocked"] == 1
    assert "attributes" not in summary["sources"]["core"]


def test_raw_other_collection_class_is_not_treated_as_a_backup_result(tmp_path, observations):
    raw, jobs = observations
    raw[0]["_source"]["collection_class"] = "inventory"
    files, manifest, path = export(tmp_path, raw, jobs)
    summary = analyse(files, manifest, path)
    assert summary["incomplete"]
    assert summary["input_quality_counts"]["unexpected_collection_class"] == 1
    assert summary["sources"]["core"]["mapping_status"]["resolved"] == 0


def test_empty_exports_are_complete_only_with_valid_manifest_and_terminal_pages(tmp_path):
    files, manifest, path = export(tmp_path, [], [])
    summary = analyse(files, manifest, path)
    assert not summary["incomplete"]
    assert all(s["action_lookup_keys"] == 0 for s in summary["sources"].values())
    assert summary["coverage_supplied"] is False


def test_output_cannot_overwrite_an_input_or_be_placed_in_input_page_directory(tmp_path, observations, capsys):
    files, manifest, path = export(tmp_path, *observations)
    arguments = ["--raw", str(tmp_path / "raw"), "--jobs", str(tmp_path / "jobs"),
                 "--output", str(tmp_path / "raw")]
    assert offline.main(arguments) == 2
    assert "output_overlaps_input" in capsys.readouterr().err
    assert not (tmp_path / "raw/summary.json").exists()


def test_changed_total_and_missing_terminal_page_are_detected(tmp_path, observations):
    files, manifest, path = export(tmp_path, *observations)
    mutate_page(files, manifest, "jobs", 1,
                lambda p: p["hits"].update({"total": {"value": 999, "relation": "eq"}}))
    summary = analyse(files, manifest, path)
    assert "changing_total" in summary["es_export"]["jobs"]["incomplete_reasons"]
    assert summary["incomplete"]

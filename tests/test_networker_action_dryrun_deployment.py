"""Container/manifest contracts and aggregate-only adapter, no cluster or ES."""

from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest
import yaml

from tools import networker_action_dryrun_container as adapter
from tools import networker_action_shadow as cli
from apps.lookups.networker_action_shadow.analysis import BASELINE
from apps.lookups.networker_action_shadow.publisher import ShadowHTTPError

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy/networker-action-dryrun"


def resources():
    return list(yaml.safe_load_all((ROOT / "k8s/job.yaml").read_text(encoding="utf-8")))


def args(output):
    return ["--mode", "dry-run", "--source", "core", "--jobs", "nw-core-job-run-2026-11",
            "--raw-start", "2026-11-09T03:00:00Z", "--raw-end", "2026-11-09T05:00:00Z", "--output", str(output)]


def report():
    return {"mode": "dry-run", "incomplete": True, "job_run_documents": 425,
            "job_types": dict(BASELINE), "mapping_status": {"unresolved": 2},
            "baseline_comparison": {"total_matches": True,
                "types": {k: {"matches": True} for k in BASELINE}}, "publisher": {"writes": 0}}


def test_yaml_job_is_one_shot_nonroot_and_no_sqlite_volume():
    config, job = resources()
    assert config["kind"] == "ConfigMap" and job["kind"] == "Job"
    assert job["apiVersion"] == "batch/v1"
    assert "namespace" not in job["metadata"]
    spec = job["spec"]
    assert spec["backoffLimit"] == 0 and spec["completions"] == spec["parallelism"] == 1
    assert "ttlSecondsAfterFinished" not in spec
    pod = spec["template"]["spec"]
    assert pod["restartPolicy"] == "Never" and pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"]
    assert all("persistentVolumeClaim" not in v for v in pod["volumes"])
    container = pod["containers"][0]
    assert len(pod["containers"]) == 1 and "initContainers" not in pod
    assert container["imagePullPolicy"] == "Never"
    assert container["securityContext"]["readOnlyRootFilesystem"]
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert set(container["resources"]) == {"requests", "limits"}


def test_manifest_uses_existing_secret_keys_service_and_ca_only():
    config, job = resources()
    existing = list(yaml.safe_load_all((ROOT / "k8s/networker-core.yaml").read_text(encoding="utf-8")))
    es = yaml.safe_load(existing[0]["data"]["collector.yaml"])["elasticsearch"]
    assert config["data"]["NW_LOOKUP_ES_URL"] == es["hosts"][0]
    old_secret = existing[1]["spec"]["template"]["spec"]["volumes"][1]["secret"]
    pod = job["spec"]["template"]["spec"]
    env = {e["name"]: e for e in pod["containers"][0]["env"]}
    for variable, key in [("NW_LOOKUP_ES_USERNAME", "elasticsearch-username"), ("NW_LOOKUP_ES_PASSWORD", "elasticsearch-password")]:
        assert env[variable]["valueFrom"]["secretKeyRef"] == {"name": old_secret["secretName"], "key": key}
    ca = next(v["secret"] for v in pod["volumes"] if "secret" in v)
    assert ca["secretName"] == old_secret["secretName"]
    assert ca["items"] == [{"key": "elasticsearch-ca-crt", "path": "ca.crt"}]
    assert env["NW_LOOKUP_ES_CA_CERTS"]["value"] == es["ca_certs"]
    assert not any("networker-core-" in str(v) for v in pod["volumes"])


def test_manifest_arguments_parse_with_real_cli_and_independent_raw_window():
    config, job = resources()
    arguments = job["spec"]["template"]["spec"]["containers"][0]["args"]
    expanded = [config["data"].get(a[2:-1], a) if a.startswith("$(") else a for a in arguments]
    parsed = cli.parser().parse_args(expanded)
    assert parsed.mode == "dry-run" and not parsed.create_shadow and parsed.state is None
    assert parsed.source == "core"
    assert parsed.jobs == ["nw-core-job-run-2026-11"]
    assert parsed.raw == ["nw-ops-raw-core-2026-11"]
    assert parsed.job_start == "2026-11-09T03:00:00Z" and parsed.job_end == "2026-11-09T05:30:00Z"
    assert parsed.raw_end == "2026-11-09T05:00:00Z"
    assert str(parsed.output).replace("\\", "/") == "/results/run"


def test_minimal_docker_code_copy_imports_without_project_dependencies(tmp_path):
    stage = tmp_path / "image-code"
    stage.mkdir()
    docker = (DEPLOY / "Dockerfile").read_text(encoding="utf-8")
    for line in docker.splitlines():
        if not line.startswith("COPY ") or line.startswith("COPY --"):
            continue
        parts = line.split()
        sources, destination = parts[1:-1], parts[-1]
        if not any(s.startswith(("apps/", "tools/")) for s in sources):
            continue
        for source in sources:
            src = ROOT / source
            assert src.exists()
            dest = stage / destination.removeprefix("./")
            if src.is_dir():
                shutil.copytree(src, dest, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                if destination.endswith("/"):
                    dest = dest / src.name
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dest)
    env = os.environ | {"PYTHONPATH": str(stage), "PYTHONDONTWRITEBYTECODE": "1"}
    process = subprocess.run([sys.executable, "-S", "tools/networker_action_shadow.py", "--help"],
                             cwd=stage, env=env, capture_output=True, text=True)
    assert process.returncode == 0 and "--raw-start" in process.stdout
    # Both the wrapper and sqlite3 import without HTTP/application packages.
    process = subprocess.run([sys.executable, "-S", "-c", "import sqlite3; from tools import networker_action_dryrun_container"],
                             cwd=stage, env=env, capture_output=True, text=True)
    assert process.returncode == 0
    assert 'import httpx, sqlite3' in docker and 'USER 10001:10001' in docker
    assert 'WORKDIR /app\n' in docker and 'PYTHONPATH=/app ' in docker
    assert '"/app/tools/networker_action_dryrun_container.py"' in docker
    assert '--no-index' in docker and '--only-binary=:all:' in docker and 'apt-get' not in docker
    assert 'COPY . ' not in docker and 'COPY apps ' not in docker
    assert not (stage / "apps/collectors").exists() and not (stage / "secrets").exists()


def test_root_dockerfile_keeps_collector_targets_and_default_stage():
    docker = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    stages = [line.split(" AS ")[-1] for line in docker.splitlines() if line.startswith("FROM ")]
    assert stages == ["base", "dxi", "dd", "i6000", "networker", "zfs"]
    ignored = (DEPLOY / "Dockerfile.dockerignore").read_text(encoding="utf-8").splitlines()
    assert "**" in ignored
    assert not any(line.startswith("!" + name) for line in ignored for name in ["input", "output", "secrets", "config"])


def test_wheel_manifest_minimal_and_sqlite_is_standard_library():
    lines = (DEPLOY / "requirements.txt").read_text(encoding="utf-8").splitlines()
    names = {line.split("==")[0] for line in lines if line and not line.startswith("#")}
    assert names == {"httpx", "httpcore", "anyio", "certifi", "idna", "h11", "typing_extensions"}
    assert "sqlite3" not in names and sqlite3.connect(":memory:").execute("SELECT 1").fetchone() == (1,)


@pytest.mark.parametrize("option", [["--mode", "shadow"], ["--state", "unused.sqlite"], ["--create-shadow"]])
def test_container_rejects_shadow_before_cli_or_network(tmp_path, monkeypatch, capsys, option):
    async def denied(_):
        raise AssertionError("must_not_run")
    monkeypatch.setattr(cli, "run", denied)
    assert adapter.main(args(tmp_path / "out") + option) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "dry_run_only"
    assert not (tmp_path / "out").exists()


def test_completed_incomplete_analysis_logs_reports_and_exits_zero(tmp_path, monkeypatch, capsys):
    value = report()
    async def run(_):
        return value
    monkeypatch.setattr(cli, "run", run)
    path = tmp_path / "out"
    assert adapter.main(args(path)) == 0
    output = capsys.readouterr().out
    assert "BEGIN_STEP4C2_SUMMARY_JSON" in output and "END_STEP4C2_SUMMARY_MD" in output
    assert '"analysis_exit_code": 3' in output and '"container_exit_code": 0' in output
    assert json.loads((path / "summary.json").read_text()) == value
    assert (path / "summary.md").exists() and not list(tmp_path.rglob("*.sqlite"))


def test_baseline_mismatch_fails_job_with_report(tmp_path, monkeypatch, capsys):
    async def run(_):
        value = report()
        value["baseline_comparison"]["total_matches"] = False
        return value
    monkeypatch.setattr(cli, "run", run)
    assert adapter.main(args(tmp_path / "out")) == 4
    assert '"baseline_matches": false' in capsys.readouterr().out
    assert (tmp_path / "out/summary.json").exists()


def test_raw_source_rejection_is_not_a_successful_container_result(tmp_path, monkeypatch, capsys):
    async def run(_):
        value = report()
        value.update(raw_hits_total=2, raw_source_accepted=1, raw_source_rejected=1,
                     raw_source_validation_passed=False)
        return value
    monkeypatch.setattr(cli, "run", run)
    assert adapter.main(args(tmp_path / "out")) == 2
    captured = capsys.readouterr().out
    assert '"baseline_matches": true' in captured
    assert '"raw_source_validation_passed": false' in captured
    assert (tmp_path / "out/summary.json").exists()


@pytest.mark.parametrize("error,expected", [(ShadowHTTPError(403), "elasticsearch_http_error"),
    (ValueError("input_mapping_not_searchable"), "input_mapping_not_searchable"),
    (RuntimeError("sensitive-sentinel-value"), "validation_or_runtime_error")])
def test_failure_reason_is_safe_and_not_mapped_to_success(tmp_path, monkeypatch, capsys, error, expected):
    async def run(_):
        raise error
    monkeypatch.setattr(cli, "run", run)
    assert adapter.main(args(tmp_path / "out")) == 2
    captured = capsys.readouterr()
    assert json.loads(captured.err)["error"] == expected
    assert "sensitive-sentinel" not in captured.err + captured.out

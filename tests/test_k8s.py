from pathlib import Path

import yaml


def test_networker_manifest_has_one_workload_with_three_basic_auth_collectors() -> None:
    documents = list(yaml.safe_load_all(Path("k8s/networker.yaml").read_text(encoding="utf-8")))
    resources = {document["kind"]: document for document in documents}

    assert set(resources) == {"ConfigMap", "Deployment", "Service"}
    assert resources["Deployment"]["metadata"]["name"] == "backup-dashboard-collector-networker"
    assert resources["Deployment"]["spec"]["replicas"] == 1

    embedded_config = yaml.safe_load(resources["ConfigMap"]["data"]["collector.yaml"])
    collectors = embedded_config["collectors"]

    assert [collector["name"] for collector in collectors] == [
        "networker_core",
        "networker_chnl",
        "networker_info",
    ]
    assert {collector["source_networker"] for collector in collectors} == {
        "core",
        "chnl",
        "info",
    }
    assert all(
        set(collector["schedule"]) == {"jobs", "backups", "inventory"}
        for collector in collectors
    )
    assert all("token_file" not in collector for collector in collectors)
    assert all(collector["username_file"] for collector in collectors)
    assert all(collector["password_file"] for collector in collectors)
    assert all(collector["ca_certs"] for collector in collectors)
    assert [collector["schedule"]["jobs"]["interval_minutes"] for collector in collectors] == [
        5,
        5,
        5,
    ]
    assert [collector["schedule"]["jobs"]["minute_offset"] for collector in collectors] == [
        3,
        1,
        4,
    ]
    assert [collector["schedule"]["backups"]["minute_offset"] for collector in collectors] == [
        6,
        9,
        12,
    ]
    assert [collector["schedule"]["inventory"]["minute_offset"] for collector in collectors] == [
        15,
        18,
        31,
    ]
    assert all(collector["schedule"]["backups"]["interval_minutes"] == 15 for collector in collectors)
    assert all(collector["schedule"]["inventory"]["interval_minutes"] == 60 for collector in collectors)
    assert all(collector["rest"]["max_concurrency"] == 1 for collector in collectors)
    assert all(collector["rest"]["request_spacing_seconds"] == 30 for collector in collectors)
    assert all(collector["rest"]["jobs_query_window_minutes"] == 10 for collector in collectors)
    assert all(collector["rest"]["backups_query_window_minutes"] == 20 for collector in collectors)
    assert all(collector["rest"]["field_filter"] for collector in collectors)

    for collector in collectors:
        schedule = collector["schedule"]
        minute_sets = {
            name: set(
                range(
                    values["minute_offset"],
                    60,
                    values["interval_minutes"],
                )
            )
            for name, values in schedule.items()
        }
        assert minute_sets["jobs"].isdisjoint(minute_sets["backups"])
        assert minute_sets["jobs"].isdisjoint(minute_sets["inventory"])
        assert minute_sets["backups"].isdisjoint(minute_sets["inventory"])

    secret_items = resources["Deployment"]["spec"]["template"]["spec"]["volumes"][1][
        "secret"
    ]["items"]
    secret_keys = {item["key"] for item in secret_items}
    assert {
        "networker-core-ca-crt",
        "networker-chnl-ca-crt",
        "networker-info-ca-crt",
    } <= secret_keys
    assert not any("token" in key or "ifrs" in key for key in secret_keys)


def test_kustomization_references_only_consolidated_networker_manifest() -> None:
    kustomization = yaml.safe_load(
        Path("k8s/kustomization.yaml").read_text(encoding="utf-8")
    )
    networker_resources = [
        resource
        for resource in kustomization["resources"]
        if resource.startswith("networker")
    ]

    assert networker_resources == ["networker.yaml"]
    assert not list(Path("k8s").glob("networker-*.yaml"))

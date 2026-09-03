from pathlib import Path

import yaml


NETWORKER_SERVERS = {
    "core": {
        "second": 0,
        "jobs_offset": 3,
        "backups_offset": 6,
        "inventory_offset": 195,
    },
    "chnl": {
        "second": 20,
        "jobs_offset": 1,
        "backups_offset": 9,
        "inventory_offset": 198,
    },
    "info": {
        "second": 40,
        "jobs_offset": 4,
        "backups_offset": 12,
        "inventory_offset": 211,
    },
}
ELASTICSEARCH_SECRET_KEYS = {
    "elasticsearch-username",
    "elasticsearch-password",
    "elasticsearch-ca-crt",
}


def _resources_for(source: str) -> dict[str, dict]:
    documents = yaml.safe_load_all(
        Path(f"k8s/networker-{source}.yaml").read_text(encoding="utf-8")
    )
    return {document["kind"]: document for document in documents}


def test_networker_has_one_workload_per_server() -> None:
    for source, expected in NETWORKER_SERVERS.items():
        resources = _resources_for(source)
        assert set(resources) == {"ConfigMap", "Deployment", "Service"}

        resource_name = f"backup-dashboard-collector-networker-{source}"
        config_map = resources["ConfigMap"]
        deployment = resources["Deployment"]
        service = resources["Service"]

        assert config_map["metadata"]["name"] == f"{resource_name}-config"
        assert deployment["metadata"]["name"] == resource_name
        assert service["metadata"]["name"] == resource_name
        assert deployment["spec"]["replicas"] == 1
        assert deployment["spec"]["strategy"] == {"type": "Recreate"}

        selector = {
            "app": "backup-dashboard-collector",
            "collector-group": "networker",
            "networker-server": source,
        }
        assert deployment["spec"]["selector"]["matchLabels"] == selector
        assert deployment["spec"]["template"]["metadata"]["labels"] == selector
        assert service["spec"]["selector"] == selector

        pod_spec = deployment["spec"]["template"]["spec"]
        assert pod_spec["hostAliases"] == [
            {
                "ip": f"networker_{source}_ip_TO_BE_FILLED",
                "hostnames": [f"networker_{source}_hostname_TO_BE_FILLED"],
            }
        ]
        assert pod_spec["containers"][0]["image"] == (
            "backup-dashboard-collector-networker:latest"
        )
        assert pod_spec["volumes"][0]["configMap"]["name"] == (
            f"{resource_name}-config"
        )

        embedded_config = yaml.safe_load(config_map["data"]["collector.yaml"])
        assert embedded_config["app"]["name"] == resource_name
        assert len(embedded_config["collectors"]) == 1
        collector = embedded_config["collectors"][0]
        assert collector["name"] == f"networker_{source}"
        assert collector["source_networker"] == source
        assert "token_file" not in collector
        assert collector["username_file"] == (
            f"/apps/secrets/networker/{source}/username"
        )
        assert collector["password_file"] == (
            f"/apps/secrets/networker/{source}/password"
        )
        assert collector["ca_certs"] == f"/apps/secrets/networker/{source}/ca.crt"

        schedule = collector["schedule"]
        assert set(schedule) == {"jobs", "backups", "inventory"}
        assert schedule["jobs"] == {
            "interval_minutes": 5,
            "minute_offset": expected["jobs_offset"],
            "second": expected["second"],
        }
        assert schedule["backups"] == {
            "interval_minutes": 15,
            "minute_offset": expected["backups_offset"],
            "second": expected["second"],
        }
        assert schedule["inventory"] == {
            "interval_minutes": 1440,
            "minute_offset": expected["inventory_offset"],
            "second": expected["second"],
        }
        assert collector["rest"] == {
            "max_concurrency": 1,
            "request_spacing_seconds": 30,
            "jobs_query_window_minutes": 10,
            "backups_query_window_minutes": 20,
            "field_filter": True,
        }

        minute_sets = {
            name: set(
                range(values["minute_offset"], 60, values["interval_minutes"])
            )
            for name, values in schedule.items()
        }
        assert minute_sets["jobs"].isdisjoint(minute_sets["backups"])
        assert minute_sets["jobs"].isdisjoint(minute_sets["inventory"])
        assert minute_sets["backups"].isdisjoint(minute_sets["inventory"])


def test_networker_workloads_share_secret_but_mount_only_their_keys() -> None:
    for source in NETWORKER_SERVERS:
        deployment = _resources_for(source)["Deployment"]
        secret = deployment["spec"]["template"]["spec"]["volumes"][1]["secret"]

        assert secret["secretName"] == (
            "backup-dashboard-collector-networker-secrets"
        )
        secret_keys = {item["key"] for item in secret["items"]}
        assert secret_keys == ELASTICSEARCH_SECRET_KEYS | {
            f"networker-{source}-ca-crt",
            f"networker-{source}-username",
            f"networker-{source}-password",
        }
        assert not any("token" in key or "ifrs" in key for key in secret_keys)
        assert not any(
            f"networker-{other}-" in key
            for other in NETWORKER_SERVERS
            if other != source
            for key in secret_keys
        )


def test_kustomization_references_three_networker_manifests() -> None:
    kustomization = yaml.safe_load(
        Path("k8s/kustomization.yaml").read_text(encoding="utf-8")
    )
    networker_resources = [
        resource
        for resource in kustomization["resources"]
        if resource.startswith("networker")
    ]

    assert networker_resources == [
        "networker-core.yaml",
        "networker-chnl.yaml",
        "networker-info.yaml",
    ]
    assert not Path("k8s/networker.yaml").exists()
    assert sorted(Path("k8s").glob("networker-*.yaml")) == [
        Path("k8s/networker-chnl.yaml"),
        Path("k8s/networker-core.yaml"),
        Path("k8s/networker-info.yaml"),
    ]

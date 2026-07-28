from pathlib import Path

import yaml


def test_networker_manifest_has_one_workload_with_four_collectors() -> None:
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
        "networker_ifrs",
    ]
    assert {collector["source_networker"] for collector in collectors} == {
        "core",
        "chnl",
        "info",
        "ifrs",
    }
    assert all(set(collector["schedule"]) == {"fast", "slow"} for collector in collectors)


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

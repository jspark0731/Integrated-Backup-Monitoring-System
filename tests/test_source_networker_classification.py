import pytest

from apps.collectors.networker_rest_collector import NetworkerRestCollector
from apps.core.config import CollectorConfig
from apps.parsers.networker_rest_parser import parse_networker_rest_payload


@pytest.mark.parametrize("source", ["core", "chnl", "info", " CHNL "])
@pytest.mark.parametrize("hostname", [None, "", "NEW-CLIENT.EXAMPLE.COM."])
def test_all_entities_use_source_without_hostname_inventory_or_workload(source, hostname):
    jobs = [
        {"id": index, "type": job_type, "clientName": hostname,
         "runOnHost": "save-client", "command": "save -g GROUP",
         "rootParentJobId": 10}
        for index, job_type in enumerate(
            ["save job", "backup action job", "workflow job"], start=1
        )
    ]
    parsed = parse_networker_rest_payload(
        {"jobs": {"jobs": jobs},
         "clients": {"clients": [{"hostname": hostname}]},
         "backups": {"backups": [{"clientHostname": hostname}]},
         # No matching jobs or backups: inventory must still use the source.
         "policies": {"protectionPolicies": [
             {"name": "idle-policy", "workflows": [{"name": "idle-workflow"}]}
         ]}},
        server_name="unrelated-server-name", source_networker=source,
    )
    for entity in ("jobs", "clients", "backups", "policies", "workflows", "monthly_report"):
        assert parsed[entity]
        for record in parsed[entity]:
            assert record["source_networker"] == source.strip().lower()
            assert record["security_domain"] == source.strip().lower()
            assert record["classification_source"] == "source_networker"
            assert record["classification_status"] == "matched_source_networker"
    save, action, workflow = parsed["jobs"]
    assert save["protection_group"] == "GROUP"
    assert action["protection_group"] == workflow["protection_group"] == ""
    assert save["client_name"] == (hostname or "save-client")
    assert action["client_name"] == workflow["client_name"] == (hostname or "")
    for record in parsed["jobs"] + parsed["clients"] + parsed["backups"]:
        assert record["client_hostname"] == record["client_name"].lower().rstrip(".")


@pytest.mark.parametrize("source", [None, "", " ", "ifrs", "unknown", "core:chnl"])
def test_invalid_source_is_an_error_even_with_core_server_name(source):
    with pytest.raises(ValueError, match="unsupported source_networker"):
        parse_networker_rest_payload({}, "networker_core", source_networker=source)


@pytest.mark.parametrize("source", ["core", "chnl", "info"])
def test_inventory_only_has_no_workload_dependency(source):
    parsed = parse_networker_rest_payload(
        {"policies": {"protectionPolicies": [
            {"name": "idle", "workflows": [{"name": "never-run"}]}
        ]}}, "server", source_networker=source,
    )
    assert parsed["jobs"] == parsed["backups"] == parsed["clients"] == []
    assert len(parsed["policies"]) == len(parsed["workflows"]) == 1
    assert parsed["policies"][0]["security_domain"] == source
    assert parsed["workflows"][0]["security_domain"] == source


@pytest.mark.asyncio
@pytest.mark.parametrize("source", [None, "", "ifrs"])
async def test_invalid_config_skips_before_any_api_request(source, monkeypatch):
    collector = NetworkerRestCollector(CollectorConfig(
        name="networker_core", type="Networker", protocol="rest", enabled=True,
        base_url="https://example.invalid", username="user", password="secret",
        source_networker=source,
    ))

    async def unexpected_request(*args):
        pytest.fail("invalid source must not trigger API requests")

    monkeypatch.setattr(collector.client, "fetch_payloads", unexpected_request)
    try:
        result = await collector.collect("jobs")
        assert result.skipped
        assert "unsupported source_networker" in result.skip_reason
        assert not result.payload
    finally:
        await collector.close()

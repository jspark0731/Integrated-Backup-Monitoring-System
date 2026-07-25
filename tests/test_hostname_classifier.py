from pathlib import Path

import pytest

from app.classifiers.hostname import HostnameClassifier
from app.parsers.networker_rest_parser import parse_networker_rest_payload


def test_classifier_matches_fqdn_short_name_and_unmapped(tmp_path: Path) -> None:
    csv_path = tmp_path / "hostname_domain.csv"
    csv_path.write_text(
        "hostname,security_domain\n"
        "core-db01.example.com,core\n"
        "chnl-web01.example.com,chnl\n",
        encoding="utf-8",
    )
    classifier = HostnameClassifier.from_csv(csv_path)

    assert classifier.classify("CORE-DB01.EXAMPLE.COM.").security_domain == "core"
    assert classifier.classify("core-db01").security_domain == "core"
    assert classifier.classify("missing-host").security_domain == "unmapped"


def test_classifier_rejects_conflicting_short_hostname_domains(tmp_path: Path) -> None:
    csv_path = tmp_path / "hostname_domain.csv"
    csv_path.write_text(
        "hostname,security_domain\n"
        "db01.core.example.com,core\n"
        "db01.chnl.example.com,chnl\n",
        encoding="utf-8",
    )
    classifier = HostnameClassifier.from_csv(csv_path)

    assert classifier.classify("db01").security_domain == "unmapped"
    assert classifier.classify("db01").status == "unmapped"


def test_classifier_rejects_unknown_domain(tmp_path: Path) -> None:
    csv_path = tmp_path / "hostname_domain.csv"
    csv_path.write_text("hostname,security_domain\nhost01,unknown\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported security domain"):
        HostnameClassifier.from_csv(csv_path)


def test_classifier_rejects_duplicate_hostname_with_different_domains(tmp_path: Path) -> None:
    csv_path = tmp_path / "hostname_domain.csv"
    csv_path.write_text(
        "hostname,security_domain\nhost01.example.com,core\nhost01.example.com,chnl\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="multiple security domains"):
        HostnameClassifier.from_csv(csv_path)


def test_networker_records_are_classified_independently_from_source(tmp_path: Path) -> None:
    csv_path = tmp_path / "hostname_domain.csv"
    csv_path.write_text(
        "hostname,security_domain\ncore-db01.example.com,core\n",
        encoding="utf-8",
    )
    classifier = HostnameClassifier.from_csv(csv_path)

    parsed = parse_networker_rest_payload(
        {
            "jobs": {
                "jobs": [
                    {
                        "id": "job-1",
                        "name": "Filesystem",
                        "state": "Completed",
                        "exitCode": 0,
                        "clientHostname": "core-db01.example.com",
                    }
                ]
            },
            "clients": {"clients": [{"clientId": "client-1", "hostname": "core-db01.example.com"}]},
            "backups": {
                "backups": [
                    {
                        "id": "backup-1",
                        "clientHostname": "core-db01.example.com",
                        "size": 100,
                    }
                ]
            },
        },
        server_name="networker_chnl",
        source_networker="chnl",
        classifier=classifier,
    )

    assert parsed["jobs"][0]["source_networker"] == "chnl"
    assert parsed["jobs"][0]["security_domain"] == "core"
    assert parsed["clients"][0]["security_domain"] == "core"
    assert parsed["monthly_report"][0]["security_domain"] == "core"

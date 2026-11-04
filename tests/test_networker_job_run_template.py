"""Offline template and document regression checks; no Elasticsearch emulation."""

import json
from fnmatch import fnmatchcase
from datetime import datetime, timezone
from pathlib import Path
import unittest

from apps.core.config import ElasticsearchConfig
from apps.models import CollectionResult
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.writers.elasticsearch import ElasticsearchWriter


TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "elasticsearch_networker_job_run_index_template.example.json"
)


class NetworkerJobRunTemplateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.template = json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))
        cls.properties = cls.template["template"]["mappings"]["properties"]

    def test_both_exit_code_paths_explicitly_support_signed_long(self) -> None:
        # Both copies must support negative codes as well as unsigned 32-bit values.
        for field in ("payload", "job"):
            with self.subTest(field=field):
                mapping = self.properties.get(field, {}).get("properties", {}).get("exit_code")
                self.assertEqual(mapping, {"type": "long"})

    def test_operational_template_composition_and_scope(self) -> None:
        self.assertEqual(self.template["index_patterns"], ["nw-*-job-run-*"])
        self.assertEqual(self.template["priority"], 400)
        self.assertEqual(self.template["composed_of"], ["nw-common-mappings"])
        self.assertIs(self.template["template"]["mappings"]["dynamic"], False)
        self.assertEqual(
            self.template["template"]["settings"],
            {"index.mapping.ignore_malformed": True},
        )
        for field in ("job", "client", "payload"):
            self.assertIs(self.properties[field]["dynamic"], False)

    def test_identifier_mappings_and_existing_payload_metadata(self) -> None:
        for field in ("source_networker", "security_domain", "derived_id", "index_month"):
            with self.subTest(field=field):
                self.assertEqual(self.properties[field], {"type": "keyword"})
        for field in ("job_id", "parent_job_id", "root_parent_job_id", "previous_job_id"):
            with self.subTest(field=field):
                self.assertEqual(self.properties["payload"]["properties"][field], {"type": "keyword"})
        for field in ("id", "parent_job_id", "root_parent_job_id", "previous_job_id"):
            with self.subTest(field=f"job.{field}"):
                self.assertEqual(self.properties["job"]["properties"][field], {"type": "keyword"})
        for field in ("server", "source_networker", "security_domain", "classification_status",
                      "classification_source", "client_hostname"):
            with self.subTest(field=field):
                self.assertEqual(self.properties["payload"]["properties"][field], {"type": "keyword"})
        self.assertEqual(self.properties["payload"]["properties"]["stopped"], {"type": "boolean"})

    def test_job_run_pattern_has_priority_over_general_job_pattern(self) -> None:
        # Recorded operational general Job template: nw-*-job-*, priority 320.
        # This is a pattern regression check, not a live template selection test.
        run_name = "nw-core-job-run-2026-11"
        self.assertTrue(fnmatchcase(run_name, "nw-*-job-*"))
        self.assertTrue(any(fnmatchcase(run_name, p) for p in self.template["index_patterns"]))
        self.assertGreater(self.template["priority"], 320)
        self.assertFalse(any(fnmatchcase("nw-core-job-2026-11", p)
                             for p in self.template["index_patterns"]))

    def test_exit_code_serialization_preserves_values_identity_and_start_month(self) -> None:
        writer = ElasticsearchWriter(ElasticsearchConfig(enabled=False))
        self.assertIsNone(writer.client)
        for exit_code in (0, 1, -1, 4294967295):
            with self.subTest(exit_code=exit_code):
                payload = parse_networker_rest_payload(
                    {"jobs": {"jobs": [{
                        "id": 3234696,
                        "type": "save job",
                        "state": "Completed",
                        "exitCode": exit_code,
                        "parentJobId": 3231071,
                        "rootParentJobId": 3231062,
                        "startTime": "2026-10-31T23:50:00Z",
                        "endTime": "2026-11-01T00:10:00Z",
                    }]}},
                    server_name="networker_core",
                    source_networker="core",
                )
                result = CollectionResult(
                    collector="networker_core",
                    target_type="Networker",
                    protocol="rest",
                    collected_at=datetime(2026, 11, 1, 0, 15, tzinfo=timezone.utc),
                    ok=True,
                    payload=payload,
                    collection_class="jobs",
                )
                actions = writer._actions_for_result(result)
                action = next(a for a in actions if a["_source"].get("document_type") == "job-run")
                encoded = json.loads(json.dumps(action, allow_nan=False))
                document = encoded["_source"]
                # With dynamic:false, every emitted analytical field needs a declaration.
                # This checks coverage, not Elasticsearch's indexing or component merge behavior.
                for field in ("job", "client", "payload"):
                    self.assertLessEqual(set(document[field]), set(self.properties[field]["properties"]))
                self.assertEqual(self.properties["run_on_host"], {"type": "keyword"})
                for field in ("payload", "job"):
                    self.assertEqual(document[field]["exit_code"], exit_code)
                    self.assertIs(type(document[field]["exit_code"]), int)
                self.assertEqual(document["payload"]["job_id"], "3234696")
                self.assertEqual(document["job"]["id"], "3234696")
                self.assertEqual(document["payload"]["parent_job_id"], 3231071)
                self.assertEqual(document["payload"]["root_parent_job_id"], 3231062)
                self.assertEqual(encoded["_index"], "nw-unmapped-job-run-2026-10")
                self.assertEqual(encoded["_id"], "networker_core:job:3234696")
                self.assertEqual(document["index_month"], "2026-10")
                self.assertEqual(document["@timestamp"], "2026-11-01T00:10:00+00:00")


if __name__ == "__main__":
    unittest.main()

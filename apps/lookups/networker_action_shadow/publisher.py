"""Isolated HTTP publisher with per-document CAS and a durable local outbox.

No transaction spans SQLite and Elasticsearch. A crash replays the pending plan;
matching full documents are skipped. This is NOT an operational publisher.
"""

from contextlib import contextmanager
from copy import deepcopy
import json
import os
from pathlib import Path
import sqlite3
from urllib.parse import quote

from apps.lookups.networker_action.adapters import Checkpoint, InMemoryLookupStore, VersionConflict
from apps.lookups.networker_action.models import digest, timestamp
from .schema import SHADOW_INDEX, lookup_schema
from .reader import PITReader


class ShadowHTTPError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__("elasticsearch_http_error")


class HTTPTransport:
    """An explicitly supplied AsyncClient; errors never include URLs or bodies."""

    def __init__(self, client):
        self.client = client

    async def request(self, method, path, **kwargs):
        try:
            response = await self.client.request(method, path, **kwargs)
        except Exception:
            raise RuntimeError("elasticsearch_transport_error") from None
        if response.status_code >= 400:
            raise ShadowHTTPError(response.status_code)
        try:
            body = response.json()
        except ValueError:
            raise RuntimeError("invalid_elasticsearch_json") from None
        if not isinstance(body, dict):
            raise RuntimeError("invalid_elasticsearch_response")
        return body


class ShadowPublisher:
    def __init__(self, http, *, index=SHADOW_INDEX, retries=3):
        if index != SHADOW_INDEX or not 1 <= retries <= 10:
            raise ValueError("shadow_target_only")
        self.http, self.index, self.retries = http, index, retries

    async def prepare(self, *, create=False):
        try:
            response = await self.http.request("GET", f"/{self.index}", params={"flat_settings": "true"})
        except ShadowHTTPError as exc:
            if exc.status != 404 or not create:
                raise
            try:
                created = await self.http.request("PUT", f"/{self.index}", json=lookup_schema())
                if created.get("acknowledged") is not True:
                    raise RuntimeError("shadow_creation_unacknowledged")
            except ShadowHTTPError as race:
                if race.status != 400:
                    raise
            response = await self.http.request("GET", f"/{self.index}", params={"flat_settings": "true"})
        if set(response) != {self.index}:
            raise ValueError("concrete_shadow_index_required")
        resource = response[self.index]
        settings, mappings = resource.get("settings", {}), resource.get("mappings", {})
        expected = lookup_schema()["mappings"]
        if (settings.get("index.mode") != "lookup" or str(settings.get("index.number_of_shards")) != "1"
                or str(settings.get("index.mapping.ignore_malformed")).lower() != "false"
                or mappings.get("dynamic") not in (False, "false")
                or mappings.get("properties") != expected["properties"]
                or resource.get("aliases")
                or any(settings.get(k, "_none") != "_none" for k in ["index.default_pipeline", "index.final_pipeline"])):
            raise ValueError("shadow_schema_mismatch")

    @staticmethod
    def validate(key, incoming, old=None):
        # Reuse STEP 4A's publication and no-candidate-loss/terminal guards.
        fake = InMemoryLookupStore()
        if old is not None:
            if old.get("action_run_key") != key:
                raise ValueError("stored_lookup_identity_mismatch")
            fake.documents[key] = deepcopy(old)
            old_time, new_time = timestamp(old.get("lookup_observed_at")), timestamp(incoming.get("lookup_observed_at"))
            if old_time and (new_time is None or new_time < old_time):
                raise ValueError("stale_evidence_requires_review")
        fake.commit({key: incoming}, expected_version=0, checkpoint=Checkpoint("shadow", "shadow"))
        return fake.documents[key]

    async def publish(self, documents):
        await self.validate_keys()
        result = {"created": 0, "updated": 0, "unchanged": 0, "cas_retries": 0}
        for key, document in sorted(documents.items()):
            if set(document) != set(lookup_schema()["mappings"]["properties"]):
                raise ValueError("lookup_field_contract_mismatch")
            if len(json.dumps(document).encode("utf-8")) > 4 * 1024 * 1024:
                raise ValueError("shadow_document_too_large")
            self.validate(key, document)
            path = f"/{self.index}/_doc/{quote(key, safe='')}"
            for attempt in range(self.retries):
                try:
                    existing = await self.http.request("GET", path)
                except ShadowHTTPError as exc:
                    if exc.status != 404:
                        raise
                    existing = None
                if existing and existing.get("_source") == document:
                    result["unchanged"] += 1
                    break
                incoming = self.validate(key, document, existing["_source"] if existing else None)
                if existing and incoming == existing["_source"]:
                    result["unchanged"] += 1
                    break
                params = {"if_seq_no": existing["_seq_no"], "if_primary_term": existing["_primary_term"]} if existing else {"op_type": "create"}
                try:
                    response = await self.http.request("PUT", path, json=incoming, params=params)
                    if response.get("result") not in {"created", "updated"} or response.get("_shards", {}).get("failed", 0):
                        raise RuntimeError("shadow_write_not_confirmed")
                    result["updated" if existing else "created"] += 1
                    break
                except ShadowHTTPError as exc:
                    if exc.status != 409:
                        raise
                    result["cas_retries"] += 1
                    if attempt == self.retries - 1:
                        raise VersionConflict("shadow_cas_exhausted") from None
        # Read-after-write verification is realtime, not dependent on refresh.
        for key, document in documents.items():
            actual = await self.http.request("GET", f"/{self.index}/_doc/{quote(key, safe='')}")
            # Conflict revocation may intentionally retain old execution metadata.
            if actual.get("_source") != self.validate(key, document, actual.get("_source")):
                raise RuntimeError("shadow_readback_mismatch")
        await self.validate_keys()
        result["readback_verified_documents"] = len(documents)
        result["replay_without_document_writes"] = result["created"] == 0 and result["updated"] == 0
        return result

    async def validate_keys(self):
        reader = PITReader(self.http)
        hits = await reader.read([self.index], "shadow", {"match_all": {}})
        keys = set()
        for hit in hits:
            doc = hit["_source"]
            key = doc.get("action_run_key")
            if not isinstance(key, str) or hit["_id"] != key or key in keys:
                raise ValueError("shadow_duplicate_or_noncanonical_key")
            self.validate(key, doc)
            keys.add(key)


class ShadowState:
    """SQLite evidence ledger + pending full plan; one local publisher at a time.

    Sensitive evidence is kept locally, never placed in aggregate reports.
    Old snapshots are retained conservatively, including unmapped copies.
    """

    def __init__(self, path, scope):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        self.db = sqlite3.connect(path, timeout=5)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS evidence (fingerprint TEXT PRIMARY KEY, channel TEXT NOT NULL, hit TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS plans (generation TEXT PRIMARY KEY, body TEXT NOT NULL, completed INTEGER NOT NULL DEFAULT 0);
        """)
        try:
            with self.lock():
                row = self.db.execute("SELECT value FROM metadata WHERE name='scope'").fetchone()
                if row and row[0] != scope:
                    raise ValueError("checkpoint_scope_mismatch")
                self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('scope', ?)", (scope,))
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    @contextmanager
    def lock(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def pending(self):
        row = self.db.execute("SELECT body FROM plans WHERE completed=0 ORDER BY rowid LIMIT 1").fetchone()
        return json.loads(row[0]) if row else None

    def hits(self, channel):
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT hit FROM evidence WHERE channel=? ORDER BY fingerprint", (channel,))]

    def stage(self, raw, jobs, make_plan, *, max_records=100000):
        with self.lock():
            if self.pending():
                return self.pending()
            for channel, records in [("raw", raw), ("jobs", jobs)]:
                for hit in records:
                    # Cursor belongs to PIT, not evidence identity.
                    clean = {k: deepcopy(hit[k]) for k in ("_index", "_id", "_source")}
                    self.db.execute("INSERT OR IGNORE INTO evidence VALUES (?, ?, ?)",
                                    (digest([channel, clean]), channel, json.dumps(clean)))
            if self.db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] > max_records:
                raise ValueError("ledger_budget_exhausted")
            raws, job_hits = self.hits("raw"), self.hits("jobs")
            generation = digest([raws, job_hits])
            row = self.db.execute("SELECT body FROM plans WHERE generation=?", (generation,)).fetchone()
            plan = json.loads(row[0]) if row else make_plan(raws, job_hits, generation)
            self.db.execute("INSERT OR IGNORE INTO plans VALUES (?, ?, 0)", (generation, json.dumps(plan)))
            return plan

    async def publish(self, publisher, plan):
        with self.lock():
            latest = self.db.execute("SELECT generation FROM plans ORDER BY rowid DESC LIMIT 1").fetchone()
            if not latest or latest[0] != plan["generation"]:
                raise ValueError("stale_local_publication_plan")
            pending = self.pending()
            if pending and pending["generation"] != plan["generation"]:
                raise ValueError("pending_plan_requires_recovery")
            result = await publisher.publish(plan["documents"])
            self.db.execute("UPDATE plans SET completed=1 WHERE generation=?", (plan["generation"],))
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('completed_generation', ?)", (plan["generation"],))
            return result

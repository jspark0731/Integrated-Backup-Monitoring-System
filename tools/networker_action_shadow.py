"""Manual STEP 4C-2 entrypoint. No scheduler/collector integration."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import ssl
import sys
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.lookups.networker_action.models import digest
from apps.lookups.networker_action_shadow.analysis import execute
from apps.lookups.networker_action_shadow.publisher import HTTPTransport, ShadowPublisher, ShadowState
from apps.lookups.networker_action_shadow.schema import SHADOW_INDEX
from apps.lookups.networker_action_shadow.reader import DEFAULT_JOB_START, DEFAULT_JOB_END


def parser():
    cli = argparse.ArgumentParser(description="Manual Action Lookup dry-run / isolated shadow")
    cli.add_argument("--mode", choices=["dry-run", "shadow"], default="dry-run")
    cli.add_argument("--source", choices=["core", "chnl", "info"], default="core")
    cli.add_argument("--jobs", nargs="+", default=["tmp-nw-job-run-4c2-20261109"])
    cli.add_argument("--raw", nargs="+", default=["nw-ops-raw-core-2026-11"])
    cli.add_argument("--job-start", default=DEFAULT_JOB_START)
    cli.add_argument("--job-end", default=DEFAULT_JOB_END)
    cli.add_argument("--raw-start", required=True, help="Separate RAW collection time bound, not Backup saveTime")
    cli.add_argument("--raw-end", required=True)
    cli.add_argument("--page-size", type=int, default=100)
    cli.add_argument("--max-hits", type=int, default=100000)
    cli.add_argument("--state", type=Path, help="Sensitive local SQLite ledger, shadow only")
    cli.add_argument("--create-shadow", action="store_true")
    cli.add_argument("--output", type=Path, required=True)
    return cli


async def run(args):
    import httpx
    if args.mode == "dry-run" and (args.state or args.create_shadow):
        raise ValueError("dry_run_cannot_publish_or_checkpoint")
    if args.mode == "shadow" and args.state is None:
        raise ValueError("durable_shadow_state_required")
    endpoint = os.environ.get("NW_LOOKUP_ES_URL", "")
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
        raise ValueError("explicit_https_es_origin_required")
    username, password = os.environ.get("NW_LOOKUP_ES_USERNAME"), os.environ.get("NW_LOOKUP_ES_PASSWORD")
    api_key = os.environ.get("NW_LOOKUP_ES_API_KEY")
    if bool(username) != bool(password) or (api_key and username):
        raise ValueError("ambiguous_es_authentication")
    ca = os.environ.get("NW_LOOKUP_ES_CA_CERTS")
    verify = ssl.create_default_context(cafile=ca) if ca else True
    # Existing ES username/password + CA/verify conventions; Writer remains separate.
    auth = (username, password) if username else None
    headers = {"Authorization": "ApiKey " + api_key} if api_key else {}
    state = None
    try:
        if args.mode == "shadow":
            state = ShadowState(args.state, digest([endpoint, args.source, SHADOW_INDEX]))
        async with httpx.AsyncClient(base_url=endpoint, auth=auth, headers=headers, verify=verify,
                                     timeout=60, follow_redirects=False, trust_env=False) as client:
            http = HTTPTransport(client)
            return await execute(http, source=args.source, job_indices=args.jobs, raw_indices=args.raw,
                job_start=args.job_start, job_end=args.job_end, raw_start=args.raw_start, raw_end=args.raw_end,
                state=state, publisher=ShadowPublisher(http) if state else None,
                create_shadow=args.create_shadow, page_size=args.page_size, max_hits=args.max_hits)
    finally:
        if state:
            state.close()


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.output.exists():
            raise ValueError("new_output_directory_required")
        # Pre-create the report directory before any publication; never replace files.
        args.output.mkdir(parents=True, mode=0o700)
        report = asyncio.run(run(args))
        with (args.output / "summary.json").open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2, ensure_ascii=True)
        with (args.output / "summary.md").open("x", encoding="utf-8") as stream:
            stream.write("# STEP 4C-2 aggregate report\n\n```json\n" + json.dumps(report, indent=2) + "\n```\n")
        print(json.dumps({"mode": report["mode"], "incomplete": report["incomplete"], "report_created": True}))
        return 3 if report["incomplete"] else 0
    except Exception:
        # No exception values, credentials, URLs, Job IDs or Backup attributes.
        print("shadow_validation_or_publication_failed; preserve_state_and_review", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

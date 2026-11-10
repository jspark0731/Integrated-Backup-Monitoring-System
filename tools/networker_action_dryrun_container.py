"""Container I/O adapter only: existing CLI parser/run, forcibly dry-run.

No analysis, ES queries, publication, checkpoint or credential logic is added.
Aggregate result markers support recovery from completed Pod logs.
"""

import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import networker_action_shadow as cli
from apps.lookups.networker_action_shadow.publisher import ShadowHTTPError

SAFE_ERRORS = frozenset({
    "new_output_directory_required", "dry_run_only", "explicit_https_es_origin_required",
    "ambiguous_es_authentication", "input_mapping_not_searchable", "explicit_input_indices_required",
    "invalid_aware_read_window", "invalid_read_limits", "missing_pit_id", "partial_pit_open",
    "partial_search", "partial_clusters", "exact_total_required", "malformed_hits", "total_mismatch",
    "source_unavailable", "duplicate_or_out_of_scope_hit", "invalid_or_reversed_cursor",
    "hit_budget_exhausted", "page_budget_exhausted", "pit_close_failed", "out_of_window_response",
    "input_source_mismatch", "elasticsearch_transport_error", "invalid_elasticsearch_json",
    "invalid_elasticsearch_response", "raw_source_validation_failed",
})


def main(argv=None):
    args = cli.parser().parse_args(argv)
    try:
        if args.mode != "dry-run" or args.state is not None or args.create_shadow:
            raise ValueError("dry_run_only")
        if args.output.exists():
            raise ValueError("new_output_directory_required")
        args.output.mkdir(parents=True, mode=0o700)
        report = asyncio.run(cli.run(args))
        # The original CLI's run() returns aggregate-only validation statistics.
        text = json.dumps(report, ensure_ascii=True, indent=2)
        markdown = "# STEP 4C-2 dry-run aggregate report\n\n```json\n" + text + "\n```\n"
        (args.output / "summary.json").write_text(text + "\n", encoding="utf-8")
        (args.output / "summary.md").write_text(markdown, encoding="utf-8")
        print("BEGIN_STEP4C2_SUMMARY_JSON\n" + text + "\nEND_STEP4C2_SUMMARY_JSON", flush=True)
        print("BEGIN_STEP4C2_SUMMARY_MD\n" + markdown + "END_STEP4C2_SUMMARY_MD", flush=True)
        comparison = report["baseline_comparison"]
        baseline_matches = comparison["total_matches"] and all(v["matches"] for v in comparison["types"].values())
        analysis_exit = 3 if report["incomplete"] else 0
        source_valid = report.get("raw_source_validation_passed", True)
        container_exit = 2 if not source_valid else 0 if baseline_matches else 4
        print(json.dumps({"analysis_exit_code": analysis_exit, "container_exit_code": container_exit,
                          "baseline_matches": baseline_matches, "incomplete": report["incomplete"],
                          "raw_source_validation_passed": source_valid,
                          "report_created": True}), flush=True)
        return container_exit
    except Exception as exc:
        # Never print exception bodies/URLs/credentials/operational attributes.
        error = str(exc) if str(exc) in SAFE_ERRORS else "validation_or_runtime_error"
        message = {"error": error, "container_exit_code": 2}
        if isinstance(exc, ShadowHTTPError):
            message.update(error="elasticsearch_http_error", http_status=exc.status)
        print(json.dumps(message), file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

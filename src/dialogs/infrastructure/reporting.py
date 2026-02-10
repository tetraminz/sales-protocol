from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from ..db import get_state
from ..report_image import write_accuracy_diff_png
from ..sgr_core import (
    METRICS_VERSION,
    all_rules,
    fixed_scan_policy,
    heatmap_zone,
    quality_thresholds,
    threshold_doc_line,
)
from ..utils import ensure_parent
from .scan_runner import _run_metrics_version, _safe_div


def _accuracy_map(conn: sqlite3.Connection, *, run_id: str) -> dict[str, float]:
    rows = conn.execute(
        "SELECT rule_key, judge_correctness FROM scan_metrics WHERE run_id=? ORDER BY rule_key",
        (run_id,),
    ).fetchall()
    return {str(row["rule_key"]): float(row["judge_correctness"]) for row in rows}


def _build_accuracy_heatmap_data(
    conn: sqlite3.Connection, *, run_id: str, rule_keys: list[str]
) -> dict[str, list[Any]]:
    conversation_ids = [
        str(row["conversation_id"])
        for row in conn.execute(
            "SELECT DISTINCT conversation_id FROM scan_results WHERE run_id=? ORDER BY conversation_id",
            (run_id,),
        ).fetchall()
    ]
    rows = conn.execute(
        """
        SELECT
          conversation_id,
          rule_key,
          SUM(CASE WHEN judge_label IS NOT NULL THEN 1 ELSE 0 END) AS judged_total,
          SUM(CASE WHEN judge_label=1 THEN 1 ELSE 0 END) AS correct_total
        FROM scan_results
        WHERE run_id=?
        GROUP BY conversation_id, rule_key
        """,
        (run_id,),
    ).fetchall()
    by_cell: dict[tuple[str, str], tuple[int, int]] = {
        (str(row["conversation_id"]), str(row["rule_key"])): (
            int(row["judged_total"] or 0),
            int(row["correct_total"] or 0),
        )
        for row in rows
    }

    scores: list[list[float | None]] = []
    judged_totals: list[list[int]] = []
    for conversation_id in conversation_ids:
        row_scores: list[float | None] = []
        row_judged: list[int] = []
        for rule_key in rule_keys:
            judged, correct = by_cell.get((conversation_id, rule_key), (0, 0))
            row_judged.append(judged)
            row_scores.append(_safe_div(float(correct), float(judged)) if judged else None)
        scores.append(row_scores)
        judged_totals.append(row_judged)

    return {
        "conversation_ids": conversation_ids,
        "rule_keys": list(rule_keys),
        "scores": scores,
        "judged_totals": judged_totals,
    }


def _heatmap_zone(score: float | None) -> str:
    return heatmap_zone(score, thresholds=quality_thresholds())


def _summarize_heatmap_zones(scores: list[list[float | None]]) -> dict[str, int]:
    counts = {"green": 0, "yellow": 0, "red": 0, "na": 0}
    for row in scores:
        for score in row:
            counts[_heatmap_zone(score)] += 1
    return counts


def _worst_heatmap_cells(
    *,
    conversation_ids: list[str],
    rule_keys: list[str],
    scores: list[list[float | None]],
    judged_totals: list[list[int]],
    limit: int = 10,
) -> list[dict[str, Any]]:
    ranked: list[tuple[float, int, str, str, int]] = []
    for row_idx, conversation_id in enumerate(conversation_ids):
        for col_idx, rule_key in enumerate(rule_keys):
            judged = int(judged_totals[row_idx][col_idx])
            score = scores[row_idx][col_idx]
            if judged <= 0 or score is None:
                continue
            ranked.append((float(score), -judged, conversation_id, rule_key, judged))
    ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    return [
        {
            "conversation_id": conversation_id,
            "rule_key": rule_key,
            "score": float(score),
            "judged_total": int(judged),
        }
        for score, _neg_judged, conversation_id, rule_key, judged in ranked[: max(0, int(limit))]
    ]


def _canonical_run_for_version(
    conn: sqlite3.Connection, *, run_id: str, metrics_version: str
) -> tuple[str, str | None]:
    canonical = get_state(conn, "canonical_run_id")
    if canonical and _run_metrics_version(conn, run_id=canonical) == metrics_version:
        return canonical, None

    row = conn.execute(
        """
        SELECT run_id
        FROM scan_runs
        WHERE status='success' AND json_extract(summary_json, '$.metrics_version')=?
        ORDER BY started_at_utc ASC
        LIMIT 1
        """,
        (metrics_version,),
    ).fetchone()
    if row is not None:
        return str(row["run_id"]), "canonical run switched to version-compatible baseline"
    return run_id, "canonical run fell back to current run due to metrics version mismatch"


def _bad_cases(conn: sqlite3.Connection, *, run_id: str, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          sr.conversation_id,
          sr.evidence_message_id,
          sr.evidence_message_order,
          sr.rule_key,
          sr.eval_hit,
          sr.judge_expected_hit,
          sr.eval_reason_code,
          sr.eval_reason,
          sr.judge_rationale,
          sr.evidence_quote,
          sr.eval_confidence,
          sr.judge_confidence,
          COALESCE(anchor.text, '') AS evidence_message_text
        FROM scan_results sr
        LEFT JOIN messages anchor ON anchor.message_id = sr.evidence_message_id
        WHERE sr.run_id=? AND sr.judge_label=0
        ORDER BY ABS(sr.eval_confidence - COALESCE(sr.judge_confidence, 0)) DESC,
                 sr.rule_key,
                 COALESCE(sr.evidence_message_order, 0),
                 sr.conversation_id
        LIMIT ?
        """,
        (run_id, int(limit)),
    ).fetchall()
    return [
        {
            "conversation_id": str(row["conversation_id"]),
            "evidence_message_id": None
            if row["evidence_message_id"] is None
            else int(row["evidence_message_id"]),
            "evidence_message_order": None
            if row["evidence_message_order"] is None
            else int(row["evidence_message_order"]),
            "rule_key": str(row["rule_key"]),
            "eval_hit": int(row["eval_hit"]),
            "judge_expected_hit": None
            if row["judge_expected_hit"] is None
            else int(row["judge_expected_hit"]),
            "eval_reason_code": str(row["eval_reason_code"]),
            "eval_reason": str(row["eval_reason"]),
            "judge_rationale": str(row["judge_rationale"] or ""),
            "evidence_quote": str(row["evidence_quote"]),
            "eval_confidence": float(row["eval_confidence"]),
            "judge_confidence": None if row["judge_confidence"] is None else float(row["judge_confidence"]),
            "evidence_message_text": str(row["evidence_message_text"]),
        }
        for row in rows
    ]


def _md_cell(text: str, max_len: int = 120) -> str:
    clipped = text.replace("\n", " ").strip()
    if len(clipped) > max_len:
        clipped = clipped[: max(0, max_len - 1)] + "…"
    return clipped.replace("|", "\\|")


def build_report(
    conn: sqlite3.Connection,
    *,
    run_id: str | None = None,
    md_path: str = "artifacts/metrics.md",
    png_path: str = "artifacts/accuracy_diff.png",
) -> dict[str, Any]:
    if run_id is None:
        row = conn.execute(
            "SELECT run_id FROM scan_runs WHERE status='success' ORDER BY started_at_utc DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise ValueError("no successful scan run found")
        run_id = str(row["run_id"])

    metrics_version = _run_metrics_version(conn, run_id=run_id) or METRICS_VERSION
    canonical_run_id, canonical_note = _canonical_run_for_version(
        conn,
        run_id=run_id,
        metrics_version=metrics_version,
    )

    rule_keys = [rule.key for rule in all_rules()]
    current = _accuracy_map(conn, run_id=run_id)
    canonical = _accuracy_map(conn, run_id=canonical_run_id)
    heatmap = _build_accuracy_heatmap_data(conn, run_id=run_id, rule_keys=rule_keys)
    conversation_ids = [str(value) for value in heatmap["conversation_ids"]]
    scores = heatmap["scores"]
    judged_totals = heatmap["judged_totals"]
    zone_counts = _summarize_heatmap_zones(scores)
    worst_cells = _worst_heatmap_cells(
        conversation_ids=conversation_ids,
        rule_keys=rule_keys,
        scores=scores,
        judged_totals=judged_totals,
    )
    bad_cases = _bad_cases(conn, run_id=run_id, limit=20)

    run_summary_row = conn.execute(
        "SELECT summary_json FROM scan_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    inserted = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])
    judged = int(
        conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_label IS NOT NULL", (run_id,)).fetchone()[0]
    )
    coverage = _safe_div(float(judged), float(inserted))

    metrics_rows = conn.execute(
        """
        SELECT rule_key, eval_total, eval_true, evaluator_hit_rate, judge_correctness, judge_coverage,
               judged_total, judge_true, judge_false
        FROM scan_metrics
        WHERE run_id=?
        ORDER BY rule_key
        """,
        (run_id,),
    ).fetchall()

    llm_rows = conn.execute(
        """
        SELECT
          phase,
          COUNT(*) AS calls,
          SUM(CASE WHEN error_message<>'' THEN 1 ELSE 0 END) AS errors,
          SUM(prompt_chars) AS prompt_chars,
          SUM(response_chars) AS response_chars
        FROM llm_calls
        WHERE run_id=?
        GROUP BY phase
        ORDER BY phase
        """,
        (run_id,),
    ).fetchall()

    ensure_parent(md_path)
    cfg = quality_thresholds()
    policy = fixed_scan_policy()
    lines = [
        "# SGR Scan Metrics",
        "",
        f"- metrics_version: `{metrics_version}`",
        (
            f"- scan_policy: `bundled={str(policy.bundle_rules).lower()}, "
            f"judge={policy.judge_mode}, context={policy.context_mode}, llm_trace={policy.llm_trace}`"
        ),
        "- llm_audit_trace: `full request_json + response_json + extracted_json`",
        f"- canonical_run_id: `{canonical_run_id}`",
        f"- current_run_id: `{run_id}`",
        f"- inserted_results: `{inserted}`",
        f"- judged_results: `{judged}`",
        f"- judge_coverage: `{coverage:.4f}`",
        f"- judge_coverage_target: `{cfg.judge_coverage_min:.2f}`",
        "",
        "## Rule Metrics",
        "",
        "| rule | eval_total | eval_true | evaluator_hit_rate | judge_correctness | judge_coverage |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    if metrics_rows:
        for row in metrics_rows:
            lines.append(
                f"| `{row['rule_key']}` | {int(row['eval_total'])} | {int(row['eval_true'])} | "
                f"{float(row['evaluator_hit_rate']):.4f} | {float(row['judge_correctness']):.4f} | "
                f"{float(row['judge_coverage']):.4f} |"
            )
    else:
        lines.append("| `-` | 0 | 0 | 0.0000 | 0.0000 | 0.0000 |")

    lines.extend(
        [
            "",
            "## Rule Quality Delta (judge_correctness)",
            "",
            "| rule | canonical | current | delta |",
            "|---|---:|---:|---:|",
        ]
    )
    for key in rule_keys:
        can = canonical.get(key, 0.0)
        cur = current.get(key, 0.0)
        delta = cur - can
        sign = "+" if delta > 0 else ""
        lines.append(f"| `{key}` | {can:.4f} | {cur:.4f} | {sign}{delta:.4f} |")

    if canonical_note:
        lines.extend(["", f"- note: {canonical_note}"])

    lines.extend(
        [
            "",
            "## LLM Calls",
            "",
            "| phase | calls | errors | prompt_chars | response_chars |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    if llm_rows:
        for row in llm_rows:
            lines.append(
                f"| `{row['phase']}` | {int(row['calls'])} | {int(row['errors'] or 0)} | "
                f"{int(row['prompt_chars'] or 0)} | {int(row['response_chars'] or 0)} |"
            )
    else:
        lines.append("| `-` | 0 | 0 | 0 | 0 |")

    lines.extend(
        [
            "",
            "## Judge-Aligned Heatmap",
            "",
            f"- thresholds: `{threshold_doc_line(thresholds=cfg)}`",
            f"- conversations: `{len(conversation_ids)}`",
            f"- rules: `{len(rule_keys)}`",
            "",
            "| zone | cells |",
            "|---|---:|",
            f"| `green` | {zone_counts['green']} |",
            f"| `yellow` | {zone_counts['yellow']} |",
            f"| `red` | {zone_counts['red']} |",
            f"| `na` | {zone_counts['na']} |",
            "",
            "### Worst conversation x rule cells",
            "",
            "| conversation_id | rule | score | judged_total |",
            "|---|---|---:|---:|",
        ]
    )

    if worst_cells:
        for cell in worst_cells:
            lines.append(
                f"| `{cell['conversation_id']}` | `{cell['rule_key']}` | {float(cell['score']):.4f} | {int(cell['judged_total'])} |"
            )
    else:
        lines.append("| `-` | `-` | n/a | 0 |")

    lines.extend(
        [
            "",
            "## Judge-Confirmed Bad Cases (judge_label=0)",
            "",
            "| conversation_id | evidence_message_id | evidence_message_order | rule | eval_hit | expected_hit | reason_code | evidence_quote |",
            "|---|---:|---:|---|---:|---:|---|---|",
        ]
    )
    if bad_cases:
        for case in bad_cases:
            lines.append(
                f"| `{case['conversation_id']}` | {case['evidence_message_id']} | {case['evidence_message_order']} | `{case['rule_key']}` | "
                f"{case['eval_hit']} | {case['judge_expected_hit']} | `{_md_cell(str(case['eval_reason_code']), 60)}` | "
                f"`{_md_cell(str(case['evidence_quote']), 80)}` |"
            )

        lines.extend(["", "### Bad Case Details", ""])
        for idx, case in enumerate(bad_cases[:10], start=1):
            lines.append(
                f"{idx}. `{case['conversation_id']}` evidence_message_id={case['evidence_message_id']} "
                f"rule=`{case['rule_key']}` eval_hit={case['eval_hit']} expected_hit={case['judge_expected_hit']}"
            )
            lines.append(f"   evidence_message_order: {_md_cell(str(case['evidence_message_order']), 40)}")
            lines.append(f"   evidence_message_text: {_md_cell(str(case['evidence_message_text']), 200)}")
            lines.append(f"   evaluator_reason: {_md_cell(str(case['eval_reason']), 200)}")
            lines.append(f"   judge_rationale: {_md_cell(str(case['judge_rationale']), 200)}")
            lines.append(f"   evidence_quote: {_md_cell(str(case['evidence_quote']), 120)}")
    else:
        lines.append("| `-` | 0 | 0 | `-` | 0 | 0 | `-` | `-` |")

    if run_summary_row is not None:
        lines.extend(["", "## Run Summary JSON", "", f"- summary_json: `{str(run_summary_row['summary_json'])}`"])

    Path(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")

    ensure_parent(png_path)
    write_accuracy_diff_png(
        png_path,
        rule_keys=rule_keys,
        conversation_ids=conversation_ids,
        scores=scores,
        thresholds=cfg,
    )

    return {
        "run_id": run_id,
        "canonical_run_id": canonical_run_id,
        "metrics_version": metrics_version,
        "rules": len(rule_keys),
        "md_path": md_path,
        "png_path": png_path,
        "bad_cases": len(bad_cases),
    }

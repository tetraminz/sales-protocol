from __future__ import annotations

import sqlite3
import struct
import uuid
import zlib
from typing import Any

from .llm import LLMClient
from .rules import active_rules
from .sgr_core import load_eval_messages, run_eval_loop
from .utils import ensure_parent, git_branch, git_commit, jdump, now_utc

def _insert_experiment_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    mode: str,
    rule_set: str,
    model: str,
    prompt_version: str,
    sgr_version: str,
) -> None:
    conn.execute(
        """
        INSERT INTO experiment_runs(
          run_id, mode, rule_set, model, git_commit, git_branch,
          prompt_version, sgr_version, started_at_utc, finished_at_utc,
          status, summary_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, '', 'running', '{}')
        """,
        (
            run_id,
            mode,
            rule_set,
            model,
            git_commit(),
            git_branch(),
            prompt_version,
            sgr_version,
            now_utc(),
        ),
    )
    conn.commit()


def _finish_experiment_run(conn: sqlite3.Connection, run_id: str, status: str, summary: dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE experiment_runs
        SET finished_at_utc=?, status=?, summary_json=?
        WHERE run_id=?
        """,
        (now_utc(), status, jdump(summary), run_id),
    )
    conn.commit()


def run_eval(
    conn: sqlite3.Connection,
    *,
    llm: LLMClient,
    mode: str,
    rule_set: str,
    prompt_version: str,
    sgr_version: str,
    conversation_from: int = 0,
    conversation_to: int = 4,
) -> str:
    if mode not in {"baseline", "sgr"}:
        raise ValueError("mode must be baseline or sgr")
    llm.require_live("run eval")

    rules = active_rules(conn)
    if not rules:
        raise ValueError("no active rules; approve or seed rules first")

    conversation_ids, msgs = load_eval_messages(
        conn,
        conversation_from=conversation_from,
        conversation_to=conversation_to,
    )

    exp_run_id = f"exp_{uuid.uuid4().hex[:12]}"
    _insert_experiment_run(
        conn,
        run_id=exp_run_id,
        mode=mode,
        rule_set=rule_set,
        model=llm.model,
        prompt_version=prompt_version,
        sgr_version=sgr_version,
    )

    llm_run_id = llm.start_run(
        conn,
        run_kind="eval",
        mode=mode,
        prompt_version=prompt_version,
        sgr_version=sgr_version,
        meta={"experiment_run_id": exp_run_id},
    )

    summary: dict[str, Any] = {
        "conversation_from": conversation_from,
        "conversation_to": conversation_to,
        "selected_conversations": len(conversation_ids),
        "messages": len(msgs),
        "rules": len(rules),
    }
    status = "failed"
    try:
        counters = run_eval_loop(
            conn,
            llm=llm,
            mode=mode,
            rules=rules,
            messages=msgs,
            conversation_ids=conversation_ids,
            llm_run_id=llm_run_id,
            exp_run_id=exp_run_id,
        )
        _compute_metrics(conn, run_id=exp_run_id)
        summary.update(counters)
        status = "success"
        return exp_run_id
    except Exception as exc:
        summary["error"] = str(exc)
        raise
    finally:
        _finish_experiment_run(conn, exp_run_id, status, summary)
        llm.finish_run(conn, llm_run_id, status)


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def _compute_metrics(conn: sqlite3.Connection, *, run_id: str) -> None:
    conn.execute("DELETE FROM experiment_metrics WHERE run_id=?", (run_id,))
    rows = conn.execute(
        """
        SELECT r.rule_key,
               SUM(CASE WHEN rr.hit=1 AND rr.judge_label=1 THEN 1 ELSE 0 END) AS tp,
               SUM(CASE WHEN rr.hit=1 AND rr.judge_label=0 THEN 1 ELSE 0 END) AS fp,
               SUM(CASE WHEN rr.hit=0 AND rr.judge_label=0 THEN 1 ELSE 0 END) AS tn,
               SUM(CASE WHEN rr.hit=0 AND rr.judge_label=1 THEN 1 ELSE 0 END) AS fn,
               COUNT(*) AS total
        FROM rule_results rr
        JOIN rules r ON r.rule_id = rr.rule_id
        WHERE rr.run_id=?
        GROUP BY r.rule_key
        ORDER BY r.rule_key
        """,
        (run_id,),
    ).fetchall()

    for row in rows:
        tp = float(row["tp"])
        fp = float(row["fp"])
        tn = float(row["tn"])
        fn = float(row["fn"])
        total = float(row["total"])

        accuracy = _safe_div(tp + tn, total)
        precision = _safe_div(tp, tp + fp)
        recall = _safe_div(tp, tp + fn)
        f1 = _safe_div(2 * precision * recall, precision + recall)
        coverage = _safe_div(tp + fp, total)

        metrics = {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "coverage": coverage,
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
            "total": total,
        }

        for name, value in metrics.items():
            conn.execute(
                "INSERT INTO experiment_metrics(run_id, rule_key, metric_name, metric_value, created_at_utc) VALUES(?, ?, ?, ?, ?)",
                (run_id, row["rule_key"], name, float(value), now_utc()),
            )

    conn.commit()


def run_diff(conn: sqlite3.Connection, *, run_a: str, run_b: str, png_path: str, md_path: str) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT run_id, rule_key, metric_value
        FROM experiment_metrics
        WHERE run_id IN (?, ?)
          AND metric_name='accuracy'
        ORDER BY rule_key, run_id
        """,
        (run_a, run_b),
    ).fetchall()
    if not rows:
        raise ValueError("no metrics found for provided run ids")

    by_rule: dict[str, dict[str, float]] = {}
    for row in rows:
        by_rule.setdefault(row["rule_key"], {})
        by_rule[row["rule_key"]][row["run_id"]] = float(row["metric_value"])

    rule_keys = sorted(by_rule.keys())
    vals_a = [by_rule[k].get(run_a, 0.0) for k in rule_keys]
    vals_b = [by_rule[k].get(run_b, 0.0) for k in rule_keys]
    deltas = [b - a for a, b in zip(vals_a, vals_b)]

    ensure_parent(png_path)
    _write_accuracy_diff_png(png_path, rule_keys, vals_a, vals_b)

    ensure_parent(md_path)
    run_meta = conn.execute(
        "SELECT run_id, mode, git_branch, git_commit, prompt_version, sgr_version FROM experiment_runs WHERE run_id IN (?, ?) ORDER BY run_id",
        (run_a, run_b),
    ).fetchall()

    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("# Run Diff Metrics\n\n")
        fh.write(f"- run_a: `{run_a}`\n")
        fh.write(f"- run_b: `{run_b}`\n\n")
        fh.write("## Run Metadata\n\n")
        fh.write("| run_id | mode | branch | commit | prompt_version | sgr_version |\n")
        fh.write("|---|---|---|---|---|---|\n")
        for row in run_meta:
            fh.write(
                f"| `{row['run_id']}` | `{row['mode']}` | `{row['git_branch']}` | `{row['git_commit']}` | `{row['prompt_version']}` | `{row['sgr_version']}` |\n"
            )

        fh.write("\n## Accuracy\n\n")
        fh.write("| rule_key | run_a | run_b | delta |\n")
        fh.write("|---|---:|---:|---:|\n")
        for i, rule_key in enumerate(rule_keys):
            fh.write(f"| `{rule_key}` | {vals_a[i]:.4f} | {vals_b[i]:.4f} | {deltas[i]:+.4f} |\n")

    return {"rules": len(rule_keys)}


def _write_accuracy_diff_png(path: str, rule_keys: list[str], vals_a: list[float], vals_b: list[float]) -> None:
    # Minimal stdlib PNG renderer: two bars per rule, no text labels.
    count = max(1, len(rule_keys))
    bar_w = 20
    gap = 12
    group_w = bar_w * 2 + gap
    margin = 24
    width = margin * 2 + group_w * count
    height = 260
    baseline = height - 24
    max_h = 190

    img = [[[255, 255, 255] for _ in range(width)] for _ in range(height)]

    # axis line
    for x in range(margin - 4, width - margin + 4):
        img[baseline][x] = [40, 40, 40]

    for i in range(count):
        x0 = margin + i * group_w
        h_a = int(max_h * max(0.0, min(1.0, vals_a[i])))
        h_b = int(max_h * max(0.0, min(1.0, vals_b[i])))

        # run_a bar (blue)
        for x in range(x0, x0 + bar_w):
            for y in range(baseline - h_a, baseline):
                if 0 <= y < height and 0 <= x < width:
                    img[y][x] = [66, 133, 244]

        # run_b bar (green)
        xb = x0 + bar_w + 4
        for x in range(xb, xb + bar_w):
            for y in range(baseline - h_b, baseline):
                if 0 <= y < height and 0 <= x < width:
                    img[y][x] = [52, 168, 83]

    _write_png_rgb(path, img)


def _write_png_rgb(path: str, img: list[list[list[int]]]) -> None:
    h = len(img)
    w = len(img[0]) if h else 0
    raw = bytearray()
    for y in range(h):
        raw.append(0)  # filter type 0
        for x in range(w):
            r, g, b = img[y][x]
            raw.extend(bytes((r, g, b)))
    comp = zlib.compress(bytes(raw), level=9)

    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)
    payload = sig + chunk(b"IHDR", ihdr) + chunk(b"IDAT", comp) + chunk(b"IEND", b"")
    with open(path, "wb") as fh:
        fh.write(payload)

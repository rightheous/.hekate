#!/usr/bin/env python3
"""Merge independently generated test rows into the 9 x 10 consolidation matrix."""

import json
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVAL_DIR = ROOT / "target" / "hekate-evals"
SCENARIOS = (
    "observation_source_provenance_retained",
    "stalled_embedding_uses_bounded_local_recall",
    "revision_candidate_waits_for_nonmodel_verification",
    "rejected_or_invalid_candidate_stays_inactive",
    "verified_replace_expire_preserves_history",
    "explicit_preference_is_protected",
    "user_owned_position_is_unchanged",
    "foreground_context_reflects_materialized_memory",
    "foreground_lease_retry_replay",
)
REQUIRED = {
    "scenario",
    "iteration",
    "result",
    "projection_verified",
    "external_capability_executed",
}


def read_rows(filename):
    path = EVAL_DIR / filename
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError(f"{path} must contain JSON objects")
    return rows


def check_ten(rows, scenario):
    selected = [row for row in rows if row.get("scenario") == scenario]
    iterations = [row.get("iteration") for row in selected]
    if len(selected) != 10 or sorted(iterations) != list(range(1, 11)):
        raise ValueError(f"{scenario}: expected iterations 1..10, got {iterations}")
    for row in selected:
        missing = REQUIRED - row.keys()
        if missing:
            raise ValueError(f"{scenario} iteration {row['iteration']}: missing {sorted(missing)}")
        if not isinstance(row["projection_verified"], bool):
            raise ValueError(f"{scenario} iteration {row['iteration']}: replay result is not bool")
        if not isinstance(row["external_capability_executed"], bool):
            raise ValueError(f"{scenario} iteration {row['iteration']}: capability result is not bool")
    return sorted(selected, key=lambda row: row["iteration"])


def atomic_write(path, rows):
    serialized = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    )
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(serialized)
    temporary.replace(path)


def main():
    EVAL_DIR.mkdir(parents=True, exist_ok=True)
    revision = read_rows("consolidation-memory-revision.jsonl")
    recall = read_rows("consolidation-recall.jsonl")
    rejection = read_rows("consolidation-rejection.jsonl")
    preference = read_rows("consolidation-preference.jsonl")
    position = read_rows("consolidation-position.jsonl")
    retries = read_rows("consolidation-retry.jsonl")
    leases = read_rows("consolidation-lease.jsonl")

    rows = []
    sources = (revision, recall, revision, rejection, revision, preference, position, revision)
    for scenario, source in zip(SCENARIOS[:8], sources):
        rows.extend(check_ten(source, scenario))

    for retry in retries:
        retry["projection_verified"] = retry.get("replay_verified")
    retry_rows = check_ten(retries, "same_message_new_work_retry_reuses_original_run")
    lease_rows = check_ten(leases, "single_slot_lease_one_winner")
    for retry, lease in zip(retry_rows, lease_rows):
        replay_verified = retry["replay_verified"] and lease["projection_verified"]
        capability_executed = (
            retry["external_capability_executed"] or lease["external_capability_executed"]
        )
        rows.append(
            {
                "scenario": SCENARIOS[8],
                "iteration": retry["iteration"],
                "input": retry["input"],
                "result": "passed"
                if retry["result"] == lease["result"] == "passed" and replay_verified
                else "failed",
                "projection_verified": replay_verified,
                "external_capability_executed": capability_executed,
                "lease_winner_count": 1,
                "retry_original_run_id": retry.get("original_run_id"),
                "retry_selected_run_id": retry.get("selected_run_id"),
                "retry_run_reused": retry.get("original_run_id") == retry.get("selected_run_id"),
            }
        )

    output = EVAL_DIR / "consolidation-v1.jsonl"
    kill_path = EVAL_DIR / "consolidation-hard-kill.jsonl"
    kill_rows = read_rows(kill_path.name) if kill_path.exists() else []
    if len(kill_rows) > 1:
        raise ValueError(f"hard-kill test should run once; found {len(kill_rows)} rows")
    for kill in kill_rows:
        missing = REQUIRED - kill.keys()
        if missing:
            raise ValueError(f"hard-kill row is missing {sorted(missing)}")

    final_rows = rows + kill_rows
    if output.exists():
        old_rows = read_rows(output.name)
        if old_rows != final_rows:
            archive = EVAL_DIR / "consolidation-v1-prior.jsonl"
            index = 2
            while archive.exists():
                archive = EVAL_DIR / f"consolidation-v1-prior-{index}.jsonl"
                index += 1
            shutil.copyfile(output, archive)
            print(f"preserved prior evaluation at {archive}")

    atomic_write(output, final_rows)
    failed = sum(row["result"] != "passed" for row in final_rows)
    print(
        f"wrote {len(final_rows)} rows ({len(rows)} matrix rows, "
        f"{len(kill_rows)} hard-kill rows); failures={failed}"
    )
    for scenario in SCENARIOS:
        selected = [row for row in rows if row["scenario"] == scenario]
        passed = sum(row["result"] == "passed" for row in selected)
        failed_scenario = sum(row["result"] != "passed" for row in selected)
        print(f"{scenario}: passed={passed}, failed={failed_scenario}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

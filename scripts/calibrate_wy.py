"""Run Phase 3 calibration scenarios on WY.csv and produce a final report.

This script is a thin driver around ``app.calibration_analysis``. It:
  1. detects the email column in WY.csv (file is not modified),
  2. runs 3 scenarios (baseline / strict / flexible),
  3. compares the analyses,
  4. picks the scenario closest to the reference target distribution,
  5. writes ``output/calibration_final/`` with the chosen thresholds,
     comparison table, and rationale.

NOTE: no pipeline logic is modified. Threshold overrides are passed via
temporary YAML configs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from app.calibration_analysis import (
    ScenarioSpec,
    analyze_run,
    compare_runs,
    detect_email_column,
    pick_best_scenario,
    run_calibration_iteration,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("calibration")

    project_root = Path(__file__).resolve().parents[1]
    # Use one of the 10 chunks as a representative calibration sample
    # (~11k rows). Running the full 114k file 3 times is too slow; the
    # distribution on an 11k random-ordered chunk is statistically
    # representative for threshold calibration.
    wy_path = project_root / "input" / "wy_chunks" / "WY_part_01.csv"
    if not wy_path.exists():
        # Fallback to the full file if chunks are not present.
        wy_path = project_root / "WY.csv"
    base_config = project_root / "configs" / "default.yaml"
    runs_root = project_root / "output" / "calibration_runs"
    final_dir = project_root / "output" / "calibration_final"

    assert wy_path.exists(), f"WY.csv not found at {wy_path}"
    email_col = detect_email_column(wy_path)
    log.info("Detected email column in WY.csv: %s", email_col)

    scenarios = [
        ScenarioSpec(
            name="A_baseline",
            high_confidence_threshold=70,
            review_threshold=40,
            description="Current production thresholds (baseline).",
        ),
        ScenarioSpec(
            name="B_strict",
            high_confidence_threshold=80,
            review_threshold=50,
            description="Stricter: harder to reach valid, tighter review band.",
        ),
        ScenarioSpec(
            name="C_flexible",
            high_confidence_threshold=60,
            review_threshold=30,
            description="More flexible: easier to reach valid, wider review band.",
        ),
    ]

    analyses: list[dict] = []
    for scen in scenarios:
        out_dir = runs_root / scen.name
        analysis = run_calibration_iteration(
            scenario=scen,
            input_file=wy_path,
            output_dir=out_dir,
            base_config_path=base_config,
            project_root=project_root,
            logger=log,
        )
        analyses.append(analysis)
        log.info(
            "Scenario %s done | valid=%.2f%% review=%.2f%% invalid=%.2f%% insights=%s",
            scen.name,
            analysis["distribution"]["valid_pct"],
            analysis["distribution"]["review_pct"],
            analysis["distribution"]["invalid_pct"],
            analysis["insights"],
        )

    # Also mirror scenario A into the requested calibration_wy_baseline dir.
    baseline_mirror = project_root / "output" / "calibration_wy_baseline"
    baseline_mirror.mkdir(parents=True, exist_ok=True)
    (baseline_mirror / "calibration_report.json").write_text(
        json.dumps(analyses[0], indent=2), encoding="utf-8"
    )

    comparison = compare_runs(*analyses)
    winner = pick_best_scenario(analyses)

    final_dir.mkdir(parents=True, exist_ok=True)
    (final_dir / "comparison.json").write_text(
        json.dumps(comparison, indent=2), encoding="utf-8"
    )
    (final_dir / "selection.json").write_text(
        json.dumps(winner, indent=2), encoding="utf-8"
    )

    chosen_name = winner["winner"]["scenario"]
    chosen_analysis = next(a for a in analyses if a["scenario"]["name"] == chosen_name)
    rationale = {
        "current_thresholds": {
            "high_confidence_threshold": 70,
            "review_threshold": 40,
        },
        "proposed_thresholds": {
            "high_confidence_threshold": chosen_analysis["scenario"]["high_confidence_threshold"],
            "review_threshold": chosen_analysis["scenario"]["review_threshold"],
        },
        "chosen_scenario": chosen_name,
        "reason": (
            "Scenario with smallest total distance to the reference "
            "target distribution (valid 60-80%, review 5-20%, invalid 15-30%)."
        ),
        "final_distribution": chosen_analysis["distribution"],
        "key_metrics": chosen_analysis["key_metrics"],
        "insights": chosen_analysis["insights"],
        "target_ranges_reference_only": {
            "valid_pct": [60.0, 80.0],
            "review_pct": [5.0, 20.0],
            "invalid_pct": [15.0, 30.0],
        },
        "note": "No production code has been modified. Apply by editing "
                "configs/default.yaml if the proposal is accepted.",
    }
    (final_dir / "final_thresholds.json").write_text(
        json.dumps(rationale, indent=2), encoding="utf-8"
    )

    print(json.dumps({
        "winner": chosen_name,
        "proposed_thresholds": rationale["proposed_thresholds"],
        "final_distribution": rationale["final_distribution"],
        "ranking": winner["ranking"],
    }, indent=2))


if __name__ == "__main__":
    main()

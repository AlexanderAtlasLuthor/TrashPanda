"""Emergency Extra Strict Offline Clean.

Post-processes a finished pipeline run directory and produces a single
*deliverable* XLSX that is sharply more conservative than V1 buckets,
without touching the network. Designed for the case where:

  * the customer reported real bounces against the V1-clean output,
  * SMTP live probing is impractical or untrustworthy (Yahoo-class
    accept-all, slow MXs, no cancel control),
  * the deadline does not allow another full re-architecture.

The filter is purely additive — it never mutates the technical CSVs
written by the pipeline. It reads them, applies an aggressive offline
filter on top of the V2 signals already present (probability, domain
risk, catch-all, smtp_status, hard_fail), and emits four artifacts:

  - ``clean_final_extra_strict.xlsx``  ← PRIMARY: hand this to the client
  - ``removed_extra_risk.xlsx``        ← what was removed and why
  - ``review_catch_all.xlsx``          ← Yahoo/AOL-class, can't confirm
  - ``cleaning_summary.txt``           ← one-page narrative
  - ``README_CLIENT.txt``              ← which file to use first

No network. No SMTP. No operator gate. The artifacts are written to
``<run_dir>/extra_strict/`` so they sit alongside the pipeline outputs
without colliding with the existing client package.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from .validation_v2.smtp_probe import DEFAULT_OPAQUE_PROVIDERS

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration                                                              #
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class ExtraStrictConfig:
    """Knobs for the Emergency Extra Strict Offline mode.

    Defaults match the recommendation in the V2 deliverability post-mortem:
    aggressive offline filter that produces a useful primary deliverable
    without depending on live SMTP. The opaque-provider list defaults to
    the set used by the SMTP probe — the same domains that cannot be
    honestly confirmed are also the ones we do not promise to the client.
    """

    min_deliverability_probability: float = 0.75
    high_risk_domain_excluded: bool = True
    medium_risk_domain_excluded: bool = True
    catch_all_excluded: bool = True
    role_based_excluded: bool = True
    opaque_providers: frozenset[str] = field(
        default_factory=lambda: frozenset(DEFAULT_OPAQUE_PROVIDERS)
    )
    output_subdir: str = "extra_strict"


# ---------------------------------------------------------------------------
# CSV → DataFrame inputs                                                     #
# ---------------------------------------------------------------------------


_INPUT_CSVS: tuple[str, ...] = (
    "clean_high_confidence.csv",
    "review_medium_confidence.csv",
    "removed_invalid.csv",
)


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOGGER.warning("extra_strict: failed to read %s (%s)", path.name, exc)
        return pd.DataFrame()


def _load_run(run_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for name in _INPUT_CSVS:
        df = _read_csv_safe(run_dir / name)
        if df.empty:
            continue
        df = df.copy()
        df["_source_csv"] = name
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out


# ---------------------------------------------------------------------------
# Column helpers                                                             #
# ---------------------------------------------------------------------------


_TRUE_TOKENS = {"1", "true", "t", "yes", "y"}


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in _TRUE_TOKENS


def _str_lower(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _domain_for(row: pd.Series) -> str:
    for key in ("corrected_domain", "domain"):
        v = _str_lower(row.get(key))
        if v:
            return v
    email = _str_lower(row.get("email"))
    return email.rpartition("@")[2] if "@" in email else ""


def _provider_class(domain: str, opaque: frozenset[str]) -> str:
    if not domain:
        return "unknown"
    if domain in opaque:
        return "yahoo_class"
    if domain in {"gmail.com", "googlemail.com"}:
        return "gmail"
    if domain in {"outlook.com", "hotmail.com", "live.com", "msn.com"}:
        return "microsoft"
    if domain in {"icloud.com", "me.com", "mac.com"}:
        return "apple"
    if domain in {"proton.me", "protonmail.com"}:
        return "proton"
    return "other"


_ROLE_PREFIXES: frozenset[str] = frozenset(
    {
        "abuse",
        "admin",
        "billing",
        "contact",
        "info",
        "marketing",
        "noreply",
        "no-reply",
        "office",
        "postmaster",
        "sales",
        "support",
        "team",
        "webmaster",
    }
)


def _is_role_based(email: str) -> bool:
    local = email.partition("@")[0].lower()
    if not local:
        return False
    return local in _ROLE_PREFIXES


# ---------------------------------------------------------------------------
# Decision logic                                                             #
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _RowDecision:
    final_action: str  # confirmed_safe | recommended_send | review_catch_all | suppress
    risk_tier: str
    reason: str
    note: str
    smtp_status: str
    confirmation_level: str
    provider_class: str
    probability: float | None
    domain: str


def _decide(row: pd.Series, config: ExtraStrictConfig) -> _RowDecision:
    email = _str_lower(row.get("email"))
    domain = _domain_for(row)
    provider = _provider_class(domain, config.opaque_providers)
    smtp_status = _str_lower(row.get("smtp_status")) or "not_tested"
    catch_all = _truthy(row.get("catch_all_flag"))
    domain_risk = _str_lower(row.get("domain_risk_level"))
    probability = _coerce_float(row.get("deliverability_probability"))
    final_action_v2 = _str_lower(row.get("final_action"))
    bucket = _str_lower(row.get("v2_final_bucket"))
    hard_fail = _truthy(row.get("hard_fail"))
    role_based = _is_role_based(email)

    confirmation_level = (
        "smtp_confirmed"
        if smtp_status == "valid"
        else "offline_only"
    )

    # ---- Hard suppress ---------------------------------------------------
    if hard_fail:
        return _RowDecision(
            "suppress", "high", "hard_fail",
            "Structural failure: invalid syntax or missing MX.",
            smtp_status, "none", provider, probability, domain,
        )
    if final_action_v2 == "auto_reject" or bucket in {"hard_fail", "duplicate"}:
        return _RowDecision(
            "suppress", "high", final_action_v2 or bucket or "rejected",
            "Pipeline rejected this row.", smtp_status, "none",
            provider, probability, domain,
        )
    if smtp_status == "invalid":
        return _RowDecision(
            "suppress", "high", "smtp_invalid",
            "MX rejected the address at probe time.",
            smtp_status, confirmation_level, provider, probability, domain,
        )

    # ---- Catch-all / opaque providers ------------------------------------
    is_yahoo_class = provider == "yahoo_class"
    if is_yahoo_class or catch_all or smtp_status == "catch_all_possible":
        # Even with high probability we cannot confirm these honestly.
        return _RowDecision(
            "review_catch_all",
            "medium",
            "catch_all_or_opaque_provider",
            (
                "Provider accepts mail without verifying the mailbox "
                "(Yahoo/AOL/Verizon-class or detected catch-all). "
                "Validate against real campaign feedback before bulk send."
            ),
            smtp_status,
            confirmation_level if confirmation_level == "smtp_confirmed" else "none",
            provider,
            probability,
            domain,
        )

    # ---- Domain risk -----------------------------------------------------
    if config.high_risk_domain_excluded and domain_risk == "high":
        return _RowDecision(
            "suppress", "high", "high_risk_domain",
            "Domain flagged high-risk by historical/feedback signals.",
            smtp_status, confirmation_level, provider, probability, domain,
        )
    if config.medium_risk_domain_excluded and domain_risk == "medium":
        return _RowDecision(
            "suppress", "medium", "medium_risk_domain",
            "Domain flagged medium-risk; excluded under extra-strict policy.",
            smtp_status, confirmation_level, provider, probability, domain,
        )

    # ---- Role-based ------------------------------------------------------
    if config.role_based_excluded and role_based:
        return _RowDecision(
            "suppress", "medium", "role_based_address",
            "Role-based local part (info@, support@, …) — not a person.",
            smtp_status, confirmation_level, provider, probability, domain,
        )

    # ---- Probability gate -----------------------------------------------
    if probability is not None and probability < config.min_deliverability_probability:
        return _RowDecision(
            "suppress", "medium", "low_probability",
            (
                f"Deliverability probability {probability:.2f} below the "
                f"extra-strict threshold {config.min_deliverability_probability:.2f}."
            ),
            smtp_status, confirmation_level, provider, probability, domain,
        )

    # ---- Confirmed / recommended ----------------------------------------
    if smtp_status == "valid":
        return _RowDecision(
            "confirmed_safe", "low", "smtp_valid_high_probability",
            "MX accepted the recipient and probability is above threshold.",
            smtp_status, "smtp_confirmed", provider, probability, domain,
        )

    return _RowDecision(
        "recommended_send", "low", "high_probability_offline",
        (
            "High deliverability probability based on offline signals "
            "(syntax, MX, domain history, scoring). Not confirmed via SMTP."
        ),
        smtp_status, "offline_only", provider, probability, domain,
    )


# ---------------------------------------------------------------------------
# DataFrame construction                                                     #
# ---------------------------------------------------------------------------


_PRIMARY_COLUMNS: tuple[str, ...] = (
    "email",
    "trashpanda_final_action",
    "trashpanda_risk_tier",
    "trashpanda_smtp_status",
    "trashpanda_confirmation_level",
    "trashpanda_provider_class",
    "trashpanda_deliverability_probability",
    "trashpanda_domain",
    "trashpanda_reason",
    "trashpanda_recommended_action",
    "trashpanda_deliverability_note",
)


def _recommended_action_for(decision: _RowDecision) -> str:
    if decision.final_action == "confirmed_safe":
        return "send"
    if decision.final_action == "recommended_send":
        return "send_with_caution"
    if decision.final_action == "review_catch_all":
        return "review"
    return "suppress"


def _build_decision_frame(
    df: pd.DataFrame, config: ExtraStrictConfig
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_PRIMARY_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, raw in df.iterrows():
        decision = _decide(raw, config)
        email = _str_lower(raw.get("email"))
        prob_str = (
            f"{decision.probability:.3f}"
            if decision.probability is not None
            else ""
        )
        out_row: dict[str, Any] = {
            "email": raw.get("email") or email,
            "trashpanda_final_action": decision.final_action,
            "trashpanda_risk_tier": decision.risk_tier,
            "trashpanda_smtp_status": decision.smtp_status,
            "trashpanda_confirmation_level": decision.confirmation_level,
            "trashpanda_provider_class": decision.provider_class,
            "trashpanda_deliverability_probability": prob_str,
            "trashpanda_domain": decision.domain,
            "trashpanda_reason": decision.reason,
            "trashpanda_recommended_action": _recommended_action_for(decision),
            "trashpanda_deliverability_note": decision.note,
        }
        # Preserve original input columns when present so the customer
        # can still merge/append their own fields.
        for key, value in raw.items():
            if key.startswith("_") or key in out_row:
                continue
            out_row[key] = value
        rows.append(out_row)

    out = pd.DataFrame(rows)
    leading = [c for c in _PRIMARY_COLUMNS if c in out.columns]
    rest = [c for c in out.columns if c not in leading]
    return out[leading + rest]


# ---------------------------------------------------------------------------
# Output writers                                                             #
# ---------------------------------------------------------------------------


def _write_xlsx(df: pd.DataFrame, path: Path, sheet: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet, index=False)


def _write_summary_text(
    out_dir: Path,
    counts: Counter[str],
    total: int,
    config: ExtraStrictConfig,
) -> Path:
    confirmed = counts.get("confirmed_safe", 0)
    recommended = counts.get("recommended_send", 0)
    catch_all = counts.get("review_catch_all", 0)
    suppressed = counts.get("suppress", 0)
    primary = confirmed + recommended

    lines = [
        "TrashPanda — Emergency Extra Strict Offline Clean",
        "",
        f"Input rows scanned:                {total}",
        f"PRIMARY deliverable rows:         {primary}",
        f"  confirmed_safe (SMTP-valid):    {confirmed}",
        f"  recommended_send (offline):     {recommended}",
        f"Review (catch-all/Yahoo-class):   {catch_all}",
        f"Removed (extra risk):             {suppressed}",
        "",
        "Policy:",
        f"  min deliverability probability: {config.min_deliverability_probability:.2f}",
        f"  exclude high-risk domains:      {config.high_risk_domain_excluded}",
        f"  exclude medium-risk domains:    {config.medium_risk_domain_excluded}",
        f"  exclude catch-all + opaque:     {config.catch_all_excluded}",
        f"  exclude role-based:             {config.role_based_excluded}",
        f"  opaque providers skipped:       {len(config.opaque_providers)}",
        "",
        "Outputs:",
        "  clean_final_extra_strict.xlsx  ← PRIMARY deliverable (send this)",
        "  removed_extra_risk.xlsx        ← excluded rows + reason",
        "  review_catch_all.xlsx          ← Yahoo/AOL-class, validate via campaign",
        "  cleaning_summary.txt           ← this file",
        "  README_CLIENT.txt              ← one-page client instructions",
    ]
    path = out_dir / "cleaning_summary.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


_README_TEMPLATE = """\
TrashPanda — Extra Strict Clean (deliverable for client)

USE THIS FILE FIRST:
  clean_final_extra_strict.xlsx

It contains every contact we are willing to recommend for immediate
send under the extra-strict policy. Each row carries explanatory
columns (trashpanda_*) that explain the verdict in plain language.

Tier breakdown
--------------
  confirmed_safe     SMTP confirmed the mailbox accepts mail.
  recommended_send   High offline probability (syntax + MX + domain
                     history). Not SMTP-confirmed. Send with normal
                     warmup pacing.

Other files
-----------
  review_catch_all.xlsx
      Yahoo / AOL / Verizon-class addresses. The provider accepts
      mail at SMTP time and may bounce afterwards. No automated
      system can confirm these without sending. Validate against
      real campaign feedback before bulk-sending.

  removed_extra_risk.xlsx
      Rows removed under the extra-strict policy with the reason
      filled in (low probability, role-based, high-risk domain,
      hard-fail, …). Useful for audit; not for sending.

  cleaning_summary.txt
      Counts and policy thresholds used for this run.

If anything looks wrong, run again with `--probability-threshold` to
tune the strictness. Default: 0.75.
"""


def _write_readme(out_dir: Path) -> Path:
    path = out_dir / "README_CLIENT.txt"
    path.write_text(_README_TEMPLATE, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Public entry point                                                         #
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class ExtraStrictResult:
    out_dir: Path
    primary_xlsx: Path
    removed_xlsx: Path
    review_xlsx: Path
    summary_txt: Path
    readme_txt: Path
    counts: dict[str, int]
    total_rows: int


def run_extra_strict_clean(
    run_dir: str | Path,
    *,
    config: ExtraStrictConfig | None = None,
) -> ExtraStrictResult:
    """Run the offline extra-strict filter against a finished run dir.

    Reads the three technical CSVs in ``run_dir`` (``clean_*``,
    ``review_*``, ``removed_*``), partitions every row into one of
    four tiers (confirmed_safe / recommended_send / review_catch_all /
    suppress), and writes the deliverable XLSXs into
    ``run_dir/<config.output_subdir>/``.
    """

    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        raise FileNotFoundError(f"run_dir does not exist: {run_dir}")

    config = config or ExtraStrictConfig()
    out_dir = run_dir / config.output_subdir
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = _load_run(run_dir)
    decisions = _build_decision_frame(raw, config)
    if decisions.empty:
        decisions = pd.DataFrame(columns=_PRIMARY_COLUMNS)

    primary_mask = decisions["trashpanda_final_action"].isin(
        ("confirmed_safe", "recommended_send")
    )
    review_mask = decisions["trashpanda_final_action"] == "review_catch_all"
    suppress_mask = decisions["trashpanda_final_action"] == "suppress"

    primary_df = decisions.loc[primary_mask].reset_index(drop=True)
    review_df = decisions.loc[review_mask].reset_index(drop=True)
    suppress_df = decisions.loc[suppress_mask].reset_index(drop=True)

    primary_xlsx = out_dir / "clean_final_extra_strict.xlsx"
    review_xlsx = out_dir / "review_catch_all.xlsx"
    removed_xlsx = out_dir / "removed_extra_risk.xlsx"

    _write_xlsx(primary_df, primary_xlsx, sheet="primary")
    _write_xlsx(review_df, review_xlsx, sheet="review")
    _write_xlsx(suppress_df, removed_xlsx, sheet="removed")

    counts = Counter(decisions["trashpanda_final_action"].tolist())
    summary_txt = _write_summary_text(out_dir, counts, len(decisions), config)
    readme_txt = _write_readme(out_dir)

    # Persist a machine-readable summary for the UI / future automation.
    json_summary = {
        "total_rows": len(decisions),
        "counts": dict(counts),
        "policy": {
            "min_deliverability_probability": config.min_deliverability_probability,
            "high_risk_domain_excluded": config.high_risk_domain_excluded,
            "medium_risk_domain_excluded": config.medium_risk_domain_excluded,
            "catch_all_excluded": config.catch_all_excluded,
            "role_based_excluded": config.role_based_excluded,
        },
        "outputs": {
            "primary_xlsx": primary_xlsx.name,
            "review_xlsx": review_xlsx.name,
            "removed_xlsx": removed_xlsx.name,
            "summary_txt": summary_txt.name,
            "readme_txt": readme_txt.name,
        },
    }
    (out_dir / "extra_strict_summary.json").write_text(
        json.dumps(json_summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return ExtraStrictResult(
        out_dir=out_dir,
        primary_xlsx=primary_xlsx,
        removed_xlsx=removed_xlsx,
        review_xlsx=review_xlsx,
        summary_txt=summary_txt,
        readme_txt=readme_txt,
        counts=dict(counts),
        total_rows=len(decisions),
    )


__all__ = [
    "ExtraStrictConfig",
    "ExtraStrictResult",
    "run_extra_strict_clean",
]

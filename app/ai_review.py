"""AI assistant for the review queue and job-level summaries.

Two public entrypoints, both deliberately small:

* ``review_queue_suggestions(emails, summary)`` — for each flagged email in the
  review queue, returns an approve / reject / uncertain recommendation with a
  confidence score and one-sentence rationale. Humans still take the final call
  in the UI; we just stack-rank the work.

* ``job_summary_narrative(summary)`` — one paragraph explaining, in plain
  English, how clean the list is and what the most notable patterns are. Meant
  as a replacement for generic copy on the Results page.

Both call Google Gemini 2.5 Flash via the ``google-genai`` SDK. We use Flash
for the free-tier generosity and the fact that the task is well-bounded
classification (engine has already computed all the signals; the model just
has to compose them into a decision).

Privacy: the local part of every email address is masked before leaving the
server (``john.doe@ameritrade.com`` → ``j***@ameritrade.com``). Domain and
V2 signals stay visible because those are the features the model actually
uses to decide — the local part is not load-bearing for the classification.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Literal

try:
    from google import genai
    from google.genai import types as genai_types
    from pydantic import BaseModel, Field
except ImportError:  # pragma: no cover - dependency is declared in requirements.txt
    genai = None  # type: ignore[assignment]
    genai_types = None  # type: ignore[assignment]
    BaseModel = object  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# Gemini 2.5 Flash has the best free-tier limits for this workload.
# Switch to gemini-2.0-flash if the newer model's quotas tighten.
_MODEL = "gemini-2.5-flash"


# ── System prompt (kept stable; no timestamps or per-job data) ───────────────

_SYSTEM_REVIEW = """You are an email list deliverability assistant embedded in a data-hygiene tool called TrashPanda.

A deterministic engine has already classified each uploaded email address. Everything in the "review queue" is an edge case the engine could not decide with high confidence. Your job is to look at the engine's signals for each address and recommend whether a human should APPROVE (keep and send to) it, REJECT (remove it), or mark it UNCERTAIN (send to manual review).

## How to decide

Use the V2 signals the engine provides. The important ones:

- `reason`: the primary reason for flagging. One of `catch-all`, `role-based`, `no-smtp`.
- `classification_bucket`: short label from the engine (e.g. "Needs attention").
- `confidence_v2` (0-1): engine's own confidence in its preliminary bucket.
- `deliverability_probability` (0-1): model-estimated probability the mailbox actually accepts mail.
- `deliverability_label`: short human label (e.g. "likely", "uncertain").
- `possible_catch_all` / `catch_all_confidence`: whether the domain accepts any address.
- `smtp_tested` / `smtp_confirmed_valid` / `smtp_suspicious` / `smtp_result`: SMTP probe outcome.
- `historical_label` / `historical_label_friendly`: past behavior of this domain.
- `review_subclass`: finer-grained subcategory (e.g. "ambiguous_role", "catch_all_unknown").
- `flags`: boolean map of extra hints.

## Decision guidelines

APPROVE when:
- `smtp_confirmed_valid` is true, OR
- `deliverability_probability` >= 0.75 AND no strong contrary signal, OR
- `historical_label` indicates a reliable/reputable domain AND `reason` is only role-based on a legitimate business role address, OR
- The engine's `final_action_label` is already "Send" or "Keep" with decent confidence, OR
- **Domain reputation fallback**: when the V2 signals are sparse (e.g. only `reason: "no-smtp"` with no `deliverability_probability` / `historical_label` / `smtp_result`), use general knowledge about the domain. Approve when the address belongs to a clearly legitimate, well-known organization — major corporations (banks, brokerages like ameritrade.com, fortune-500 companies), government (.gov), accredited universities (.edu), established consumer providers (gmail.com, outlook.com, yahoo.com, icloud.com, proton.me). The local part also matters here: `firstname.lastname@bigcorp.com` looks like a real employee, while `xqz9831@bigcorp.com` doesn't.

REJECT when:
- `smtp_result` explicitly rejected (hard bounce, no such user), OR
- `deliverability_probability` <= 0.25 with additional negative signals, OR
- `historical_label` is "risky" / "blacklisted" / "bouncing", OR
- `reason_codes_v2` includes `disposable`, `placeholder`, `fake`, or `invalid_domain`, OR
- The domain is obviously throwaway / disposable based on its name (e.g. `mailinator.com`, `tempmail.*`, `guerrillamail.*`, `10minutemail.*`).

UNCERTAIN when signals genuinely conflict (e.g. high deliverability_probability but historical_label is risky), or when both the V2 signals AND the domain itself are ambiguous (small unfamiliar domain with no reputation data either way). UNCERTAIN should be the *exception* for cases the human really needs to look at — not the default for "no signals". If you're returning UNCERTAIN for more than ~20% of items, you're being too cautious; re-check whether domain reputation alone justifies an approve or reject.

Role-based addresses (info@, admin@, support@) are NOT automatic rejects. Many B2B campaigns intentionally target them. Approve when the domain looks healthy; mark uncertain when it doesn't.

Catch-all domains (the server accepts every address) are the opposite: rarely outright rejects, often uncertain. The mailbox may or may not exist. Use secondary signals (SMTP probe, historical data) to decide.

## Output format

For each email in the input list, return one decision object:

- `id`: the email's id field, copied verbatim
- `decision`: "approve" | "reject" | "uncertain"
- `confidence`: 0.0 to 1.0 — how sure you are about this specific call
- `reasoning`: one short sentence (max 140 chars), plain English, no jargon, no emojis. Reference the actual signals OR the domain reputation you used (e.g. "ameritrade.com is a major US brokerage, address looks like a real employee" or "gmail address with valid local part"). Avoid hedging filler like "based on the provided signals".

You are a first-pass filter that stack-ranks the queue, not an auto-approver — but also not a rubber-stamp "uncertain" generator. Make a real call when you reasonably can. Save UNCERTAIN for genuine edge cases.
"""

_SYSTEM_SUMMARY = """You are a data-hygiene assistant that writes short, plain-English summaries of an email cleaning job.

You will be given the aggregate outputs of a deterministic cleaning pipeline: totals, percentages, and top reason counts. Your job is to turn that into one short paragraph a marketer can read in five seconds.

## What to include

- An overall health verdict ("excellent shape", "good shape", "needs attention", "has serious issues"). Base this on the ready-to-send percentage: >=95% excellent, 85-95% good, 70-85% needs attention, <70% serious issues.
- The one or two most actionable numbers — e.g. how many records are ready, how many need review, how many are high-risk.
- If review queue is non-trivial, mention the dominant reason or dominant domain if provided.
- A short next-step hint (e.g. "approve the flagged role-based addresses in bulk" or "drop the high-risk removals and send").

## What to avoid

- Robotic phrasing ("based on the provided data..."). Write like a smart colleague.
- Numbers without context ("2094 records"). Pair numbers with meaning ("2094 high-risk addresses removed - about 1.7% of your list").
- Multiple paragraphs. ONE paragraph, 2-4 sentences, ~60 words max.
- Redundant restatement of every number. The UI already shows them as tiles; you're adding the story.
- Emojis. No emojis.
- Hedging. No "seems", "appears", "might be". Be direct.

## Output format

Return ONLY the paragraph text. No headings, no bullets, no preamble like "Summary:". Plain prose.
"""


# ── Pydantic schemas for structured output ───────────────────────────────────
#
# Gemini reads `response_json_schema=Schema.model_json_schema()` and returns
# JSON in response.text that we validate back into the same Schema.

if genai is not None:
    class _Suggestion(BaseModel):
        id: str = Field(description="The id field copied verbatim from the input item.")
        decision: Literal["approve", "reject", "uncertain"]
        confidence: float = Field(ge=0.0, le=1.0)
        reasoning: str = Field(max_length=240)

    class _SuggestionList(BaseModel):
        suggestions: list[_Suggestion]


# ── PII masking ──────────────────────────────────────────────────────────────

def _mask_email(email: str) -> str:
    """Return ``j***@domain.com`` for ``john.doe@domain.com``.

    We keep the domain intact (the model needs it for the decision) and the
    first letter of the local part (helps recognizably identify personal vs
    role addresses like ``info@`` vs ``a***@``). Anything else is elided.
    """
    if "@" not in email:
        return email
    local, _, domain = email.partition("@")
    if not local:
        return "***@" + domain
    return local[0] + "***@" + domain


# ── Signal whitelist ─────────────────────────────────────────────────────────
#
# Only forward the fields the model actually uses to decide. Don't ship the
# full row — keeps payloads small and less PII-exposing.

_SIGNAL_FIELDS = (
    "reason",
    "confidence",
    "classification_bucket",
    "bucket_label",
    "bucket_v2",
    "confidence_v2",
    "confidence_tier",
    "deliverability_probability",
    "deliverability_label",
    "deliverability_factors",
    "final_action",
    "final_action_label",
    "decision_reason",
    "decision_note",
    "decision_confidence",
    "historical_label",
    "historical_label_friendly",
    "possible_catch_all",
    "catch_all_confidence",
    "catch_all_reason",
    "review_subclass",
    "smtp_tested",
    "smtp_confirmed_valid",
    "smtp_suspicious",
    "smtp_result",
    "smtp_code",
    "smtp_confidence",
    "reason_codes_v2",
    "flags",
)


def _compact_email_for_model(email: dict[str, Any]) -> dict[str, Any]:
    """Strip to just the signals the model should see, with PII masked."""
    out: dict[str, Any] = {
        "id": email["id"],
        "email_masked": _mask_email(email["email"]),
        "domain": email.get("domain", ""),
    }
    for key in _SIGNAL_FIELDS:
        if key in email and email[key] not in (None, "", [], {}):
            out[key] = email[key]
    return out


# ── Public entrypoints ───────────────────────────────────────────────────────


class AIUnavailable(RuntimeError):
    """Raised when the AI endpoints can't run — missing SDK or API key."""


def _client() -> "genai.Client":
    if genai is None:
        raise AIUnavailable(
            "google-genai SDK is not installed. Run `pip install google-genai`."
        )
    # Gemini accepts either GEMINI_API_KEY (preferred) or GOOGLE_API_KEY.
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise AIUnavailable(
            "GEMINI_API_KEY is not set in the environment. "
            "Grab a free key at https://aistudio.google.com/apikey and set it in the .env file at the repo root."
        )
    return genai.Client(api_key=api_key)


def review_queue_suggestions(
    emails: list[dict[str, Any]],
    job_summary: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Ask Gemini for an approve/reject/uncertain call on each flagged email.

    Returns a list of ``{id, decision, confidence, reasoning}`` dicts in the
    same order as the input. On total failure raises ``AIUnavailable`` or the
    underlying google-genai exception — the caller decides how to surface that
    to the user.
    """
    client = _client()

    if not emails:
        return []

    compact = [_compact_email_for_model(e) for e in emails]

    # User message: the batch. The reusable decision rules live in the system
    # instruction, so Gemini's implicit prefix cache hits them across calls.
    # The per-job batch is fresh every request and never caches — correct.
    user_payload = {
        "job_summary_totals": _compact_summary(job_summary),
        "review_queue": compact,
        "instructions": (
            "Return one suggestion per item in review_queue, in the same order. "
            "Copy each input item's `id` verbatim into the output."
        ),
    }
    user_text = json.dumps(user_payload, sort_keys=True, ensure_ascii=False)

    response = client.models.generate_content(
        model=_MODEL,
        contents=user_text,
        config=genai_types.GenerateContentConfig(
            system_instruction=_SYSTEM_REVIEW,
            response_mime_type="application/json",
            response_schema=_SuggestionList,
            max_output_tokens=4096,
            temperature=0.0,
        ),
    )

    parsed = response.parsed
    if parsed is None:
        # Fall back to parsing response.text if the SDK didn't auto-parse.
        raw = (response.text or "").strip()
        parsed = _SuggestionList.model_validate_json(raw) if raw else _SuggestionList(suggestions=[])
    _log_cache_usage("ai_review", response)

    # Re-key by id so the UI doesn't have to rely on list order, and drop any
    # hallucinated ids the model made up that aren't in the original batch.
    known_ids = {e["id"] for e in emails}
    out: list[dict[str, Any]] = []
    for s in parsed.suggestions:
        if s.id not in known_ids:
            continue
        out.append({
            "id": s.id,
            "decision": s.decision,
            "confidence": round(float(s.confidence), 3),
            "reasoning": s.reasoning,
        })
    return out


def job_summary_narrative(job_summary: dict[str, Any]) -> str:
    """One-paragraph plain-English summary of a completed job.

    Input is the same ``summary`` dict the /jobs/{id} endpoint already returns.
    Output is a single paragraph suitable to render directly in the UI.
    """
    client = _client()

    user_text = json.dumps(
        {"summary": _compact_summary(job_summary)},
        sort_keys=True,
        ensure_ascii=False,
    )

    response = client.models.generate_content(
        model=_MODEL,
        contents=user_text,
        config=genai_types.GenerateContentConfig(
            system_instruction=_SYSTEM_SUMMARY,
            max_output_tokens=256,
            temperature=0.3,
        ),
    )
    _log_cache_usage("ai_summary", response)

    return (response.text or "").strip()


# ── Helpers ──────────────────────────────────────────────────────────────────


def _compact_summary(s: dict[str, Any] | None) -> dict[str, Any]:
    """Trim the job summary to the fields the model cares about."""
    if not s:
        return {}
    keep = (
        "total_input_rows",
        "total_valid",
        "total_review",
        "total_invalid_or_bounce_risk",
        "duplicates_removed",
        "typo_corrections",
        "disposable_emails",
        "placeholder_or_fake_emails",
        "role_based_emails",
    )
    return {k: s[k] for k in keep if k in s and s[k] is not None}


def _log_cache_usage(tag: str, response: Any) -> None:
    """Best-effort debug log so we can verify the system prompt is caching."""
    usage = getattr(response, "usage_metadata", None)
    if not usage:
        return
    logger.info(
        "%s usage: prompt=%s cached=%s output=%s total=%s",
        tag,
        getattr(usage, "prompt_token_count", "?"),
        getattr(usage, "cached_content_token_count", "?"),
        getattr(usage, "candidates_token_count", "?"),
        getattr(usage, "total_token_count", "?"),
    )

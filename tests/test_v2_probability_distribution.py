"""Distribution-level tests for the additive deliverability probability model.

Complements the unit and integration tests by verifying **statistical
properties** of the output distribution over synthetic row sets:

  * no cluster contains more than 10% of rows at a single exact value
    (i.e. the output is no longer collapsed into a few discrete values);
  * at least some values land in each of high / medium / low on a
    realistic mixed input;
  * identical signal sets on different rows spread continuously via
    the deterministic per-row noise, not a flat line.
"""

from __future__ import annotations

from collections import Counter

from app.validation_v2.probability.row_model import (
    DeliverabilityInputs,
    compute_deliverability_probability,
)


def _mk(email: str, **kw: object) -> DeliverabilityInputs:
    defaults: dict[str, object] = {
        "score_post_history": 70,
        "historical_label": "neutral",
        "confidence_adjustment_applied": 0,
        "catch_all_confidence": 0.0,
        "possible_catch_all": False,
        "smtp_result": "not_tested",
        "smtp_confidence": 0.0,
        "has_mx_record": True,
        "has_a_record": False,
        "domain_match": False,
        "typo_detected": False,
        "hard_fail": False,
        "v2_final_bucket": "review",
        "email": email,
        "domain": email.split("@", 1)[-1] if "@" in email else "",
    }
    defaults.update(kw)
    return DeliverabilityInputs(**defaults)  # type: ignore[arg-type]


def _emails(n: int, prefix: str = "user", domain: str = "example.com") -> list[str]:
    return [f"{prefix}{i}@{domain}" for i in range(n)]


def test_identical_signals_spread_via_deterministic_noise() -> None:
    """200 rows with the SAME V2 signals but different emails should not
    collapse to a single probability value — the deterministic noise
    spreads them continuously."""
    probs = [
        compute_deliverability_probability(_mk(e)).probability
        for e in _emails(200)
    ]
    # No value should appear more than 10% of the time.
    counts = Counter(round(p, 3) for p in probs)
    most_common_count = counts.most_common(1)[0][1]
    assert most_common_count / len(probs) <= 0.10, (
        f"probability collapsed: most common value appears "
        f"{most_common_count}/{len(probs)} times"
    )
    # At least 30 distinct rounded values in a sample of 200.
    assert len(counts) >= 30


def test_mixed_inputs_produce_all_three_labels() -> None:
    """A realistic mix of row types must populate all three labels."""
    samples: list[DeliverabilityInputs] = []
    for i, e in enumerate(_emails(30, domain="gmail.com")):
        samples.append(_mk(e, historical_label="historically_reliable",
                           smtp_result="deliverable", domain_match=True))
    for i, e in enumerate(_emails(30, domain="neutral.com")):
        samples.append(_mk(e))  # baseline mx only
    for i, e in enumerate(_emails(30, domain="risky.net")):
        samples.append(_mk(e, historical_label="historically_risky",
                           possible_catch_all=True, catch_all_confidence=0.7))
    for i, e in enumerate(_emails(10, domain="dead.zone")):
        samples.append(_mk(e, has_mx_record=False, has_a_record=False,
                           smtp_result="undeliverable"))

    labels = Counter(
        compute_deliverability_probability(s).label for s in samples
    )
    assert labels["high"] >= 1
    assert labels["medium"] >= 1
    assert labels["low"] >= 1


def test_probability_is_continuous_not_discrete() -> None:
    """Rolling 500 rows across varied signals should hit at least 100
    distinct probability values when rounded to 3 decimals — evidence
    that the model is no longer producing only a handful of clusters."""
    samples: list[DeliverabilityInputs] = []
    for i, e in enumerate(_emails(125, domain="a.com")):
        samples.append(_mk(e))
    for i, e in enumerate(_emails(125, domain="b.com")):
        samples.append(_mk(e, historical_label="historically_reliable"))
    for i, e in enumerate(_emails(125, domain="c.com")):
        samples.append(_mk(e, smtp_result="inconclusive"))
    for i, e in enumerate(_emails(125, domain="d.com")):
        samples.append(_mk(e, historical_label="historically_unstable",
                           catch_all_confidence=0.45))

    probs = [compute_deliverability_probability(s).probability for s in samples]
    distinct = len({round(p, 3) for p in probs})
    assert distinct >= 100, f"only {distinct} distinct rounded probabilities in 500 samples"


def test_runs_are_stable_across_invocations() -> None:
    """Run the same input set twice; distributions must be identical."""
    emails = _emails(100)
    run_1 = [compute_deliverability_probability(_mk(e)).probability for e in emails]
    run_2 = [compute_deliverability_probability(_mk(e)).probability for e in emails]
    assert run_1 == run_2

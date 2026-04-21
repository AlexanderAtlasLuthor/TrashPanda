"""DNS utilities for Subphase 5: MX/A resolution with in-memory per-run cache.

Responsibilities:
- Resolve MX records for a domain; fall back to A/AAAA when configured.
- Cache results by corrected_domain for the duration of one pipeline run.
- Enrich a DataFrame chunk with per-row DNS signal columns.

No SMTP probing. No inbox verification. No scoring. No final decisions.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import dns.exception
import dns.resolver
import pandas as pd


@dataclass(slots=True)
class DnsResult:
    """Structured result of a single DNS lookup for one domain."""

    dns_check_performed: bool
    domain_exists: bool
    has_mx_record: bool
    has_a_record: bool
    dns_error: str | None


class DnsCache:
    """In-memory per-run cache keyed by corrected_domain.

    domains_queried: actual DNS network calls made (unique new domains).
    cache_hits: unique domain lookups served from cache without a network call.
    """

    def __init__(self) -> None:
        self._store: dict[str, DnsResult] = {}
        self.domains_queried: int = 0
        self.cache_hits: int = 0

    def get(self, domain: str) -> DnsResult | None:
        """Return cached result or None. No metric side-effects."""
        return self._store.get(domain)

    def set(self, domain: str, result: DnsResult) -> None:
        """Store a result and count one real query."""
        self._store[domain] = result
        self.domains_queried += 1

    def __contains__(self, domain: str) -> bool:
        return domain in self._store

    def __len__(self) -> int:
        return len(self._store)


def resolve_domain_dns(
    domain: str,
    timeout_seconds: float = 4.0,
    fallback_to_a_record: bool = True,
) -> DnsResult:
    """Query DNS for one domain: MX first, A/AAAA as fallback if configured.

    domain_exists is True only when at least one useful record is found
    (MX or A/AAAA). NXDOMAIN, timeouts, and no-records yield domain_exists=False.

    Error semantics:
    - nxdomain       : domain not found in DNS
    - timeout        : DNS query exceeded lifetime
    - no_nameservers : no nameservers available for the domain
    - no_mx          : no MX and A fallback is disabled
    - no_mx_no_a     : no MX, no A, no AAAA (but not NXDOMAIN)
    - error          : unexpected exception
    """
    resolver = dns.resolver.Resolver()
    resolver.lifetime = timeout_seconds

    # --- MX pass ---
    try:
        resolver.resolve(domain, "MX")
        return DnsResult(
            dns_check_performed=True,
            domain_exists=True,
            has_mx_record=True,
            has_a_record=False,
            dns_error=None,
        )
    except dns.resolver.NXDOMAIN:
        return DnsResult(
            dns_check_performed=True,
            domain_exists=False,
            has_mx_record=False,
            has_a_record=False,
            dns_error="nxdomain",
        )
    except dns.exception.Timeout:
        return DnsResult(
            dns_check_performed=True,
            domain_exists=False,
            has_mx_record=False,
            has_a_record=False,
            dns_error="timeout",
        )
    except dns.resolver.NoNameservers:
        return DnsResult(
            dns_check_performed=True,
            domain_exists=False,
            has_mx_record=False,
            has_a_record=False,
            dns_error="no_nameservers",
        )
    except dns.resolver.NoAnswer:
        pass  # domain exists but has no MX; try A/AAAA below
    except Exception:
        return DnsResult(
            dns_check_performed=True,
            domain_exists=False,
            has_mx_record=False,
            has_a_record=False,
            dns_error="error",
        )

    # --- A / AAAA fallback ---
    if not fallback_to_a_record:
        return DnsResult(
            dns_check_performed=True,
            domain_exists=False,
            has_mx_record=False,
            has_a_record=False,
            dns_error="no_mx",
        )

    for rdtype in ("A", "AAAA"):
        try:
            resolver.resolve(domain, rdtype)
            return DnsResult(
                dns_check_performed=True,
                domain_exists=True,
                has_mx_record=False,
                has_a_record=True,
                dns_error=None,
            )
        except dns.resolver.NXDOMAIN:
            # Unexpected after a NoAnswer on MX, but handle safely.
            return DnsResult(
                dns_check_performed=True,
                domain_exists=False,
                has_mx_record=False,
                has_a_record=False,
                dns_error="nxdomain",
            )
        except dns.exception.Timeout:
            return DnsResult(
                dns_check_performed=True,
                domain_exists=False,
                has_mx_record=False,
                has_a_record=False,
                dns_error="timeout",
            )
        except (dns.resolver.NoAnswer, dns.resolver.NoNameservers):
            continue
        except Exception:
            continue

    # Domain exists in DNS (no NXDOMAIN) but has no MX, no A, no AAAA.
    return DnsResult(
        dns_check_performed=True,
        domain_exists=False,
        has_mx_record=False,
        has_a_record=False,
        dns_error="no_mx_no_a",
    )


def _resolve_batch(
    domains: set[str],
    timeout_seconds: float,
    fallback_to_a_record: bool,
    max_workers: int,
) -> dict[str, DnsResult]:
    """Resolve a set of new (uncached) domains concurrently via ThreadPoolExecutor."""
    workers = min(max_workers, len(domains))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_domain = {
            executor.submit(resolve_domain_dns, d, timeout_seconds, fallback_to_a_record): d
            for d in domains
        }
        results: dict[str, DnsResult] = {}
        for future in as_completed(future_to_domain):
            domain = future_to_domain[future]
            try:
                results[domain] = future.result()
            except Exception:
                results[domain] = DnsResult(
                    dns_check_performed=True,
                    domain_exists=False,
                    has_mx_record=False,
                    has_a_record=False,
                    dns_error="error",
                )
    return results


def apply_dns_enrichment_column(
    frame: pd.DataFrame,
    cache: DnsCache,
    timeout_seconds: float = 4.0,
    fallback_to_a_record: bool = True,
    max_workers: int = 20,
) -> pd.DataFrame:
    """Add DNS signal columns to a chunk using the shared cache.

    Eligible rows: syntax_valid=True AND corrected_domain is not null.
    All other rows receive pd.NA for boolean DNS columns and None for dns_error.

    Cache is updated with any newly resolved domains. Cache hit counting is
    done at domain level (not row level): one cache hit per unique domain
    already known, regardless of how many rows share that domain.
    """
    result = frame.copy()

    for col in ("dns_check_performed", "domain_exists", "has_mx_record", "has_a_record"):
        result[col] = pd.NA
    result["dns_error"] = None

    # Guard: upstream columns must exist to determine eligibility.
    if "syntax_valid" not in result.columns or "corrected_domain" not in result.columns:
        for col in ("dns_check_performed", "domain_exists", "has_mx_record", "has_a_record"):
            result[col] = result[col].astype("boolean")
        return result

    eligible_mask = result["syntax_valid"].eq(True) & result["corrected_domain"].notna()
    eligible_indices = result.index[eligible_mask].tolist()

    if not eligible_indices:
        for col in ("dns_check_performed", "domain_exists", "has_mx_record", "has_a_record"):
            result[col] = result[col].astype("boolean")
        return result

    all_domains: set[str] = {result.loc[idx, "corrected_domain"] for idx in eligible_indices}
    new_domains: set[str] = {d for d in all_domains if d not in cache}

    # Domain-level cache hit accounting.
    cache.cache_hits += len(all_domains) - len(new_domains)

    if new_domains:
        new_results = _resolve_batch(new_domains, timeout_seconds, fallback_to_a_record, max_workers)
        for domain, dns_result in new_results.items():
            cache.set(domain, dns_result)

    # Map per-domain results back to every eligible row.
    for idx in eligible_indices:
        domain = result.loc[idx, "corrected_domain"]
        dns_result = cache.get(domain)
        if dns_result is None:
            continue
        result.loc[idx, "dns_check_performed"] = dns_result.dns_check_performed
        result.loc[idx, "domain_exists"] = dns_result.domain_exists
        result.loc[idx, "has_mx_record"] = dns_result.has_mx_record
        result.loc[idx, "has_a_record"] = dns_result.has_a_record
        result.loc[idx, "dns_error"] = dns_result.dns_error

    for col in ("dns_check_performed", "domain_exists", "has_mx_record", "has_a_record"):
        result[col] = result[col].astype("boolean")

    return result

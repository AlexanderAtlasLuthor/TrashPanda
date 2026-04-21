"""SignalEvaluator: the abstract base for a single signal evaluator.

Concrete evaluators read one row (a plain Python dict) and produce zero
or more ``ScoringSignal`` instances. They must not mutate their input
and must not depend on pandas — the row is a pure dict so the scoring
core stays decoupled from any specific dataframe library.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .signal import ScoringSignal


class SignalEvaluator(ABC):
    """Abstract base class for a single signal evaluator.

    Subclasses override ``name`` (a stable identifier) and implement
    ``evaluate``. Returning an empty list means "this evaluator found
    nothing applicable on this row" — the engine treats that as a
    neutral contribution, not an error.
    """

    name: str = ""

    @abstractmethod
    def evaluate(self, row: dict) -> list["ScoringSignal"]:
        """Return 0..N signals for ``row``.

        Implementations must not modify ``row``. They must not import
        pandas. Accepting a plain dict keeps the evaluator universe
        testable with minimal fixtures and portable to non-pandas
        callers (streaming producers, unit tests, etc.).
        """
        raise NotImplementedError

"""Development bootstrap utility for the SaaS persistence schema."""

from __future__ import annotations

from sqlalchemy import Engine

from .base import Base
from .session import dispose_engine, get_engine

# Import models for side effects so metadata is fully populated before create_all.
from . import models as _models  # noqa: F401


def init_db(engine: Engine | None = None) -> None:
    """Create all configured SaaS tables on the target engine."""

    target_engine = engine or get_engine()
    Base.metadata.create_all(bind=target_engine)


def main() -> None:
    init_db()
    dispose_engine()


if __name__ == "__main__":
    main()


"""Package d'orchestration Dagster du pipeline Local Bike.

Expose `defs` pour `dagster dev -m orchestration`.
"""

from orchestration.definitions import defs

__all__ = ["defs"]

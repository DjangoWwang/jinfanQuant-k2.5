"""User-specified custom weight allocation model."""

from __future__ import annotations

import pandas as pd

from app.engine.allocation.base import AllocationModel


class CustomWeightModel(AllocationModel):
    """Apply user-specified target weights.

    Parameters
    ----------
    target_weights:
        Mapping of fund identifier to desired weight.  Values are
        normalised to sum to 1.0 on construction.
    """

    def __init__(self, target_weights: dict[str, float]) -> None:
        total = sum(target_weights.values()) or 1.0
        self._weights = {k: v / total for k, v in target_weights.items()}

    def calculate_weights(
        self, returns: pd.DataFrame, **kwargs
    ) -> dict[str, float]:
        """Return the pre-set custom weights.

        Only funds present in *returns* are included; weights are
        re-normalised if some funds are missing.
        """
        available = {
            col: self._weights[col]
            for col in returns.columns
            if col in self._weights
        }
        if not available:
            return {}
        total = sum(available.values()) or 1.0
        return {k: v / total for k, v in available.items()}

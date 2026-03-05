"""Equal-weight (1/N) allocation model."""

from __future__ import annotations

import pandas as pd

from app.engine.allocation.base import AllocationModel


class EqualWeightModel(AllocationModel):
    """Assign equal weight to every fund (1/N strategy)."""

    def calculate_weights(
        self, returns: pd.DataFrame, **kwargs
    ) -> dict[str, float]:
        n = len(returns.columns)
        if n == 0:
            return {}
        w = 1.0 / n
        return {col: w for col in returns.columns}

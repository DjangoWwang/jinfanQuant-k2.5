"""Risk-parity allocation model.

The risk-parity (equal risk contribution) strategy allocates weights
so that each fund contributes equally to the total portfolio variance.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from app.engine.allocation.base import AllocationModel


def _risk_budget_objective(
    weights: np.ndarray, cov: np.ndarray
) -> float:
    """Objective: minimise the sum of squared differences between each
    asset's risk contribution and the target (equal) risk budget."""
    n = len(weights)
    port_var = weights @ cov @ weights
    if port_var <= 0:
        return 0.0
    # Marginal risk contribution
    marginal = cov @ weights
    # Risk contribution of each asset
    rc = weights * marginal / np.sqrt(port_var)
    target_rc = np.sqrt(port_var) / n
    return float(np.sum((rc - target_rc) ** 2))


class RiskParityModel(AllocationModel):
    """Equal-risk-contribution allocation using ``scipy.optimize``."""

    def __init__(self, lookback_days: int = 252) -> None:
        self.lookback_days = lookback_days

    def calculate_weights(
        self, returns: pd.DataFrame, **kwargs
    ) -> dict[str, float]:
        """Compute risk-parity weights from the trailing covariance matrix.

        Parameters
        ----------
        returns:
            Daily (or periodic) return ``DataFrame``.  The last
            ``lookback_days`` rows are used to estimate the covariance
            matrix.

        Returns
        -------
        Weights dict summing to 1.0.
        """
        cols = list(returns.columns)
        n = len(cols)
        if n == 0:
            return {}
        if n == 1:
            return {cols[0]: 1.0}

        # Use the trailing window
        tail = returns[cols].tail(self.lookback_days).dropna()
        if len(tail) < 2:
            # Fallback to equal weight when insufficient data
            w = 1.0 / n
            return {c: w for c in cols}

        cov = tail.cov().values

        # Initial guess: equal weight
        x0 = np.ones(n) / n
        bounds = tuple((1e-6, 1.0) for _ in range(n))
        constraints = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}

        result = minimize(
            _risk_budget_objective,
            x0,
            args=(cov,),
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 500, "ftol": 1e-12},
        )

        if result.success:
            w = result.x / result.x.sum()
        else:
            # Fallback: inverse-volatility weighting
            vols = np.sqrt(np.diag(cov))
            vols = np.where(vols == 0, 1e-8, vols)
            inv_vol = 1.0 / vols
            w = inv_vol / inv_vol.sum()

        return {cols[i]: float(w[i]) for i in range(n)}

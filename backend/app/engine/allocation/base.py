"""Abstract base class for portfolio allocation models."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class AllocationModel(ABC):
    """Base class that every allocation strategy must implement.

    Subclasses override :meth:`calculate_weights` to return a mapping of
    fund identifiers to target weights (values summing to 1.0).
    """

    @abstractmethod
    def calculate_weights(
        self, returns: pd.DataFrame, **kwargs
    ) -> dict[str, float]:
        """Compute target allocation weights.

        Parameters
        ----------
        returns:
            A ``DataFrame`` of periodic returns where each column is a
            fund and each row is a date.
        **kwargs:
            Strategy-specific parameters.

        Returns
        -------
        A dict mapping fund identifier (column name) to its target
        weight.
        """
        ...

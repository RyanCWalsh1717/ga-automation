"""
accounting_utils.py — Shared financial math helpers
====================================================
Centralises rounding so every JE amount, accrual, and fee calculation
uses the same accounting convention throughout the pipeline.

Python 3's built-in round() uses banker's rounding (IEEE 754 half-to-even),
which is correct for statistical work but wrong for accounting: a stream of
$x.005 values will drift relative to the expected $x.01 half-up result,
producing reconciliation ghosts at scale.

Use _round(x) everywhere a dollar amount is computed for output.
Use _round(x, 4) for intermediate unit-rates (daily rates, etc.) where
higher precision is kept until the final JE line is assembled.
"""

from decimal import Decimal, ROUND_HALF_UP


def _round(value: float, places: int = 2) -> float:
    """
    Round a float to *places* decimal digits using HALF_UP (accounting standard).

    Examples
    --------
    >>> _round(2.5)    # banker's round() → 2,  _round → 3.0  ✓
    3.0
    >>> _round(1.005)  # banker's round() → 1.0, _round → 1.01 ✓
    1.01
    >>> _round(2.675)  # banker's round() → 2.67, _round → 2.68 ✓
    2.68
    """
    if value is None:
        return 0.0
    quantizer = Decimal(10) ** -places          # e.g. Decimal('0.01') for places=2
    return float(Decimal(str(value)).quantize(quantizer, rounding=ROUND_HALF_UP))

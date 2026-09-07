"""
Quality control checks for polar radar datasets.

Each check adds a boolean ``qc_<name>`` variable on ``slow_time`` where
``True`` means the trace passed, and updates ``qc``, the element-wise AND of
every check run so far. Use :func:`run_qc` to run several checks at once.

Built-in checks and the variables they need:

- :func:`ice_thickness_threshold` — ``standard:surface``, ``standard:bottom``
- :func:`snr_bed_pick` — ``Data``, ``standard:bottom``
- :func:`heading_change` — ``Latitude``, ``Longitude``, and ``Heading``
- :func:`minimum_agl` — ``standard:surface``

Missing inputs are filled in automatically where possible: :func:`ensure_picks`
loads layer picks through an ``OPRConnection``, and :func:`ensure_heading`
reconstructs ``Heading`` from GPS positions (course over ground) for seasons
that did not record one. NaN inputs fail the corresponding check.

Custom checks are plain functions that take a dataset, build a boolean mask on
``slow_time`` and return :func:`apply_qc_mask`::

    def roll_check(ds, max_roll_deg=5.0):
        mask = xr.DataArray(np.abs(np.degrees(ds["Roll"].values)) <= max_roll_deg,
                            dims="slow_time")
        return apply_qc_mask(ds, mask, "roll")

    run_qc(ds, checks={"ice_thickness_threshold": {}, roll_check: {"max_roll_deg": 5}})
"""

from .checks import apply_qc_mask as apply_qc_mask
from .checks import ensure_heading as ensure_heading
from .checks import ensure_picks as ensure_picks
from .checks import heading_change as heading_change
from .checks import heading_rate as heading_rate
from .checks import ice_thickness_threshold as ice_thickness_threshold
from .checks import minimum_agl as minimum_agl
from .checks import snr_bed_pick as snr_bed_pick
from .runner import run_qc as run_qc

__all__ = [
    "apply_qc_mask",
    "ensure_heading",
    "ensure_picks",
    "heading_change",
    "heading_rate",
    "ice_thickness_threshold",
    "minimum_agl",
    "run_qc",
    "snr_bed_pick",
]

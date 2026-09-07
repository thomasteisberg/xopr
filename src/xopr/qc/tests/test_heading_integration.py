"""
Network integration tests for GPS heading reconstruction on real OPR frames.

Thresholds are pinned with margin from the results of
scripts/validate_gps_heading.py.
"""

import numpy as np
import pytest

import xopr
from xopr.qc import heading_change, run_qc

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="module")
def opr():
    return xopr.OPRConnection(cache_dir="radar_cache")


def _line(opr, collection, segment=None, n=2):
    items = opr.query_frames(collections=[collection], segment_paths=segment and [segment], max_items=n)
    return xopr.merge_frames(opr.load_frames(items))


def test_gps_heading_matches_measured(opr):
    """2019_Antarctica_GV has an INS heading; the GPS course should agree."""
    ds = _line(opr, "2019_Antarctica_GV", "20191105_01")
    assert ds.attrs.get("heading_source") is None
    gps = xopr.add_heading(ds, overwrite=True)["Heading"].values
    diff = (gps - ds["Heading"].values + np.pi) % (2 * np.pi) - np.pi
    assert np.isnan(gps).mean() < 0.01
    assert np.degrees(np.nanpercentile(np.abs(diff), 90)) < 10.0

    meas = heading_change(ds, source="measured")["qc_heading_change"].values
    recon = heading_change(ds, source="gps")["qc_heading_change"].values
    assert (meas == recon).mean() >= 0.97


def test_run_qc_on_season_without_heading(opr):
    """2012_Greenland_P3 (legacy MATLAB) has no Heading; run_qc must reconstruct it."""
    ds = _line(opr, "2012_Greenland_P3")
    assert "Heading" not in ds
    out = run_qc(ds, checks={"heading_change": {}})
    assert out.attrs["heading_source"] == "gps"
    assert "GPS" in out["Heading"].attrs["source"]
    assert np.isnan(out["Heading"].values).mean() < 0.05
    assert "qc_heading_change" in out

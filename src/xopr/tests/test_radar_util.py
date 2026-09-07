"""Tests for xopr.radar_util.add_heading (synthetic tracks, no network)."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from xopr.radar_util import add_heading

N = 200
R_EARTH = 6371e3


def track(lat, lon, dt_s=1.0):
    t = pd.to_datetime(np.arange(len(lat)) * dt_s, unit="s")
    return xr.Dataset(
        {"Latitude": ("slow_time", np.asarray(lat, float)),
         "Longitude": ("slow_time", np.asarray(lon, float))},
        coords={"slow_time": t},
    )


def north():
    return track(np.linspace(-75, -74.9, N), np.full(N, 100.0))


def wrapped_diff_deg(h):
    return np.degrees((np.diff(h) + np.pi) % (2 * np.pi) - np.pi)


@pytest.mark.parametrize("lat,lon,expected", [
    (np.linspace(-75, -74.9, N), np.full(N, 100.0), 0.0),
    (np.full(N, -75.0), np.linspace(100, 100.3, N), 90.0),
    (np.linspace(-74.9, -75, N), np.full(N, 100.0), 180.0),
    (np.full(N, -75.0), np.linspace(100.3, 100, N), -90.0),
])
def test_cardinal_directions(lat, lon, expected):
    h = np.degrees(add_heading(track(lat, lon))["Heading"].values)
    diff = (h - expected + 180) % 360 - 180
    assert np.all(np.abs(diff) < 0.05)


def test_circle_rate():
    th = np.linspace(0, 2 * np.pi, N, endpoint=False)
    r = 5000.0
    lat = -75 + np.degrees(r * np.sin(th) / R_EARTH)
    lon = 100 + np.degrees(r * np.cos(th) / (R_EARTH * np.cos(np.radians(-75))))
    h = add_heading(track(lat, lon), smooth_m=0)["Heading"].values
    assert not np.isnan(h).any()
    step = wrapped_diff_deg(h)[1:-1]
    assert np.allclose(np.abs(step), 360 / N, rtol=0.01)


def test_wrap_and_antimeridian():
    lon = 179.9 + np.linspace(0, 0.2, N)  # eastward across the antimeridian
    lon[lon > 180] -= 360
    h = add_heading(track(np.full(N, -75.0), lon))["Heading"].values
    assert np.all(np.abs(wrapped_diff_deg(h)) < 0.1)
    assert np.all(np.abs(h) <= np.pi)


def test_near_pole():
    ds = track(np.linspace(-89.95, -89.9, N), np.full(N, 45.0))
    h = add_heading(ds)["Heading"].values
    assert np.isfinite(h).all()
    assert np.all(np.abs(wrapped_diff_deg(h)) < 0.1)


def test_stationary_and_nan_blocks():
    lat = np.linspace(-75, -74.9, N)
    lat[80:120] = lat[80]
    lat[150:160] = np.nan
    h = add_heading(track(lat, np.full(N, 100.0)))["Heading"].values
    assert np.isnan(h[85:115]).all()
    assert np.isnan(h[150:160]).all()
    assert np.isfinite(h[:75]).all() and np.isfinite(h[125:145]).all()


def test_time_gap_only_at_gap():
    ds = north()
    t = ds.slow_time.values.copy()
    t[100:] += np.timedelta64(60, "s")
    h = add_heading(ds.assign_coords(slow_time=t))["Heading"].values
    nan_idx = np.where(np.isnan(h))[0]
    assert set(nan_idx) == {99, 100}


def test_isolated_nan_filled():
    lat = np.linspace(-75, -74.9, N)
    lat[100] = np.nan
    h = add_heading(track(lat, np.full(N, 100.0)))["Heading"].values
    assert np.isfinite(h).all()


def test_numeric_slow_time():
    ds = north().assign_coords(slow_time=np.arange(N, dtype=float))
    assert np.isfinite(add_heading(ds)["Heading"].values).all()


def test_non_monotonic_raises():
    ds = north()
    t = ds.slow_time.values.copy()
    t[[10, 11]] = t[[11, 10]]
    with pytest.raises(ValueError, match="monotonic"):
        add_heading(ds.assign_coords(slow_time=t))


def test_missing_position_raises():
    with pytest.raises(ValueError, match="Longitude"):
        add_heading(north().drop_vars("Longitude"))


def test_existing_heading_preserved_unless_overwrite():
    ds = north()
    ds["Heading"] = ("slow_time", np.ones(N))
    assert np.all(add_heading(ds)["Heading"].values == 1.0)
    assert "heading_source" not in add_heading(ds).attrs
    out = add_heading(ds, overwrite=True)
    assert np.allclose(out["Heading"].values, 0.0, atol=1e-3)
    assert out.attrs["heading_source"] == "gps"
    assert "GPS" in out["Heading"].attrs["source"]


def test_all_nan_heading_treated_as_missing():
    ds = north()
    ds["Heading"] = ("slow_time", np.full(N, np.nan))
    assert np.isfinite(add_heading(ds)["Heading"].values).all()


def test_copy_semantics():
    ds = north()
    add_heading(ds)
    assert "Heading" not in ds

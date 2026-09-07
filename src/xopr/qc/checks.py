"""
Quality control checks for polar radar datasets.

Each check function takes an xarray Dataset and returns a modified copy
with a per-trace boolean mask added as a new variable.
"""

import warnings

import numpy as np
import xarray as xr
from pyproj import Transformer
from scipy.constants import c as speed_of_light

from xopr.radar_util import add_heading

_REQUIRED_LAYERS = {
    "standard:surface": [":surface"],
    "standard:bottom": [":bottom"],
}


def _resolve_layer(name, aliases, available_keys):
    """Return the first key in *available_keys* that matches *name* or an alias."""
    if name in available_keys:
        return name
    for alias in aliases:
        if alias in available_keys:
            return alias
    return None


def ensure_picks(ds, opr=None):
    """
    Ensure ``standard:surface`` and ``standard:bottom`` variables exist.

    If either variable is missing, layer picks are loaded via
    ``opr.get_layers()`` and their twtt values are assigned to the
    dataset.  The layer names ``":surface"`` and ``":bottom"`` are
    accepted as aliases and are stored under their canonical
    ``standard:`` names.

    Parameters
    ----------
    ds : xarray.Dataset
        Radar dataset, potentially missing pick variables.
    opr : xopr.OPRConnection, optional
        An OPR connection used to fetch layers. Required only when
        pick variables are missing from *ds*.

    Returns
    -------
    xarray.Dataset
        Copy of *ds* with ``standard:surface`` and ``standard:bottom``
        variables present.

    Raises
    ------
    ValueError
        If picks are missing and *opr* is ``None``, or if the required
        layers cannot be loaded.
    """
    if all(v in ds for v in _REQUIRED_LAYERS):
        return ds

    if opr is None:
        missing = [v for v in _REQUIRED_LAYERS if v not in ds]
        raise ValueError(
            f"Dataset is missing {missing} and no OPRConnection was "
            "provided to load layer picks. Pass an opr= argument or "
            "add the variables to the dataset manually."
        )

    ds = ds.copy()
    layers = opr.get_layers(ds)
    if layers is None:
        raise ValueError("No layer data found for this dataset.")

    for canonical, aliases in _REQUIRED_LAYERS.items():
        if canonical in ds:
            continue
        matched = _resolve_layer(canonical, aliases, layers)
        if matched is None:
            raise ValueError(
                f"Layer '{canonical}' (or aliases {aliases}) not found. "
                f"Available layers: {list(layers.keys())}"
            )
        twtt = layers[matched]["twtt"]
        twtt = twtt.reindex(
            slow_time=ds.slow_time,
            method="nearest",
            tolerance=np.timedelta64(5, "s"),
            fill_value=np.nan,
        )
        ds[canonical] = twtt

    return ds


def ensure_heading(ds, source="auto", nan_warn_fraction=0.05, **kwargs):
    """
    Ensure a usable ``Heading`` variable exists, reconstructing it from GPS
    positions when the season did not record one.

    Parameters
    ----------
    ds : xarray.Dataset
        Radar dataset.
    source : {"auto", "measured", "gps"}, optional
        ``"auto"`` keeps a measured heading if present (and not all-NaN),
        otherwise reconstructs one. ``"gps"`` always reconstructs (course
        over ground, see :func:`xopr.radar_util.add_heading`).
        ``"measured"`` never reconstructs and raises if missing.
    nan_warn_fraction : float, optional
        Warn when more than this fraction of the reconstructed heading is
        NaN (those traces fail ``heading_change``).
    **kwargs
        Passed to :func:`xopr.radar_util.add_heading`.

    Returns
    -------
    xarray.Dataset
        Copy of *ds* with ``Heading`` and ``attrs['heading_source']`` set to
        ``"measured"`` or ``"gps"``.

    Raises
    ------
    ValueError
        If *source* is invalid, if ``source="measured"`` and no heading is
        present, or if reconstruction is impossible (no ``Latitude`` /
        ``Longitude``).
    """
    if source not in ("auto", "measured", "gps"):
        raise ValueError(f"source must be 'auto', 'measured' or 'gps', got {source!r}")
    measured = "Heading" in ds and not np.all(np.isnan(ds["Heading"].values))
    if source == "measured" or (source == "auto" and measured):
        if not measured:
            raise ValueError(
                "Dataset has no measured 'Heading'. Use source='auto' or 'gps' to "
                "reconstruct it from GPS positions with xopr.radar_util.add_heading."
            )
        if ds.attrs.get("heading_source") != "measured":
            ds = ds.copy()
            ds.attrs["heading_source"] = "measured"
        return ds
    ds = add_heading(ds, overwrite=True, **kwargs)
    nan_frac = float(np.isnan(ds["Heading"].values).mean())
    if nan_frac > nan_warn_fraction:
        warnings.warn(
            f"{nan_frac:.1%} of GPS-reconstructed Heading is NaN; those traces will "
            "fail heading_change.", UserWarning, stacklevel=2,
        )
    return ds


def apply_qc_mask(ds, mask, name):
    """
    Add a QC mask to a dataset and update the combined ``qc`` variable.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    mask : xarray.DataArray
        Boolean DataArray with dimension ``slow_time``. True means the
        trace passed the check.
    name : str
        Check name; stored as ``qc_{name}`` in the returned dataset.

    Returns
    -------
    xarray.Dataset
        Copy of *ds* with ``qc_{name}`` added. The combined ``qc``
        variable is the element-wise AND of all individual masks.
    """
    ds = ds.copy()
    var_name = f"qc_{name}"
    ds[var_name] = mask
    if "qc" in ds:
        ds["qc"] = ds["qc"] & mask
    else:
        ds["qc"] = mask.copy()
    return ds


# ---- Individual checks -----------------------------------------------


def ice_thickness_threshold(ds, min_thickness_m=500.0, epsilon_ice=3.15):
    """
    Flag traces where ice thickness is below a minimum threshold.

    Ice thickness is computed from the ``standard:surface`` and
    ``standard:bottom`` two-way travel time picks using the radar wave
    speed in ice.

    Parameters
    ----------
    ds : xarray.Dataset
        Must contain ``standard:surface`` and ``standard:bottom``
        variables (units: seconds).
    min_thickness_m : float, optional
        Minimum ice thickness in metres. Default 500.
    epsilon_ice : float, optional
        Relative permittivity of ice. Default 3.15.

    Returns
    -------
    xarray.Dataset
        Copy with ``qc_ice_thickness_threshold`` and ``qc`` variables.

    Raises
    ------
    ValueError
        If ``standard:surface`` or ``standard:bottom`` is missing.
    """
    for var in _REQUIRED_LAYERS:
        if var not in ds:
            raise ValueError(f"Dataset is missing required variable '{var}'")

    v_ice = speed_of_light / np.sqrt(epsilon_ice)
    thickness = (ds["standard:bottom"] - ds["standard:surface"]) * v_ice / 2.0
    mask = xr.DataArray(
        thickness.values >= min_thickness_m,
        dims="slow_time",
    )
    # NaN picks produce NaN thickness → comparison is False
    return apply_qc_mask(ds, mask, "ice_thickness_threshold")


def snr_bed_pick(ds, min_snr_db=5.0, noise_region_samples=50):
    """
    Flag traces where the bed pick signal-to-noise ratio is too low.

    SNR is estimated as the ratio of bed-pick power to the noise floor.
    The noise floor is taken as the mean power over the last
    *noise_region_samples* fast-time samples (assumed to be below the
    bed return).

    Parameters
    ----------
    ds : xarray.Dataset
        Must contain ``Data`` (dims ``slow_time``, ``twtt``) and
        ``standard:bottom`` (units: seconds).
    min_snr_db : float, optional
        Minimum acceptable SNR in dB. Default 5.
    noise_region_samples : int, optional
        Number of fast-time samples at the end of each trace to use
        for the noise-floor estimate. Default 50.

    Returns
    -------
    xarray.Dataset
        Copy with ``qc_snr_bed_pick`` and ``qc`` variables.

    Raises
    ------
    ValueError
        If ``Data`` or ``standard:bottom`` is missing.
    """
    for var in ("Data", "standard:bottom"):
        if var not in ds:
            raise ValueError(f"Dataset is missing required variable '{var}'")

    data = np.abs(ds["Data"].values)  # (slow_time, twtt) or (twtt, slow_time)
    # Ensure shape is (slow_time, twtt)
    if ds["Data"].dims[0] == "twtt":
        data = data.T

    twtt = ds.twtt.values
    bottom_twtt = ds["standard:bottom"].values

    n_traces = data.shape[0]
    idx = np.clip(np.searchsorted(twtt, bottom_twtt), 0, data.shape[1] - 1)
    bed_power = np.where(np.isnan(bottom_twtt), np.nan, data[np.arange(n_traces), idx])

    noise_floor = np.nanmean(data[:, -noise_region_samples:], axis=1)

    with np.errstate(divide="ignore", invalid="ignore"):
        snr_db = 10.0 * np.log10(bed_power / noise_floor)

    mask = xr.DataArray(snr_db >= min_snr_db, dims="slow_time")
    return apply_qc_mask(ds, mask, "snr_bed_pick")


def heading_rate(ds):
    """
    Absolute heading rate of change along track, in degrees per kilometre.

    Parameters
    ----------
    ds : xarray.Dataset
        Must contain ``Heading`` (radians) and ``Latitude`` / ``Longitude``
        (degrees).

    Returns
    -------
    xarray.DataArray
        Rate on ``slow_time``; the first trace is 0. NaN where ``Heading``
        is NaN.
    """
    for var in ("Heading", "Latitude", "Longitude"):
        if var not in ds:
            raise ValueError(f"Dataset is missing required variable '{var}'")

    heading_rad = ds["Heading"].values
    lat, lon = ds["Latitude"].values, ds["Longitude"].values
    epsg = "EPSG:3031" if np.nanmean(lat) < 0 else "EPSG:3413"
    x, y = Transformer.from_crs("EPSG:4326", epsg, always_xy=True).transform(lon, lat)
    dist_m = np.sqrt(np.diff(x) ** 2 + np.diff(y) ** 2)

    dh = np.diff(heading_rad)
    dh = (dh + np.pi) % (2 * np.pi) - np.pi  # wrap to [-π, π]
    dh_deg = np.abs(np.degrees(dh))
    with np.errstate(divide="ignore", invalid="ignore"):
        deg_per_km = np.where(dist_m > 0, dh_deg / (dist_m / 1000.0), 0.0 * dh_deg)

    rate = np.concatenate([[0.0], deg_per_km])
    return xr.DataArray(rate, dims="slow_time", attrs={"units": "degrees/km"})


def heading_change(ds, max_deg_per_km=5.0, source="auto", **heading_kwargs):
    """
    Flag traces with rapid aircraft heading changes.

    The heading rate of change is estimated from consecutive traces and
    normalised by along-track distance. If the dataset has no ``Heading``
    (older seasons), one is reconstructed from GPS positions via
    :func:`ensure_heading`; the reconstruction is course over ground, which
    differs from the true heading by a slowly varying crab angle that
    cancels in the rate. NaN headings fail the check.

    Parameters
    ----------
    ds : xarray.Dataset
        Must contain ``Latitude`` / ``Longitude`` (degrees) and, unless
        reconstructed, ``Heading`` (radians).
    max_deg_per_km : float, optional
        Maximum acceptable heading change in degrees per kilometre.
        Default 5: at typical 15–30 m trace spacing, the trace-to-trace rate
        on straight P3 lines routinely exceeds 2 deg/km from INS/GPS jitter.
    source : {"auto", "measured", "gps"}, optional
        Heading source policy, see :func:`ensure_heading`.
    **heading_kwargs
        Passed to :func:`xopr.radar_util.add_heading` when reconstructing.

    Returns
    -------
    xarray.Dataset
        Copy with ``qc_heading_change`` and ``qc`` variables (and
        ``Heading`` if it was reconstructed).

    Raises
    ------
    ValueError
        If ``Latitude`` or ``Longitude`` is missing, or ``Heading`` is
        missing with ``source="measured"``.
    """
    ds = ensure_heading(ds, source=source, **heading_kwargs)
    rate = heading_rate(ds)
    mask = xr.DataArray(rate.values <= max_deg_per_km, dims="slow_time")
    return apply_qc_mask(ds, mask, "heading_change")


def minimum_agl(ds, min_agl_m=100.0):
    """
    Flag traces where the above-ground level is too low.

    AGL is estimated from the ``standard:surface`` two-way travel time
    as the one-way range from the platform to the surface.

    Parameters
    ----------
    ds : xarray.Dataset
        Must contain ``standard:surface`` (units: seconds).
    min_agl_m : float, optional
        Minimum above-ground level in metres. Default 100.

    Returns
    -------
    xarray.Dataset
        Copy with ``qc_minimum_agl`` and ``qc`` variables.

    Raises
    ------
    ValueError
        If ``standard:surface`` is missing.
    """
    if "standard:surface" not in ds:
        raise ValueError("Dataset is missing required variable 'standard:surface'")

    agl = ds["standard:surface"].values * speed_of_light / 2.0
    mask = xr.DataArray(agl >= min_agl_m, dims="slow_time")
    return apply_qc_mask(ds, mask, "minimum_agl")

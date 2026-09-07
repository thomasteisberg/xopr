"""
Validate GPS-reconstructed heading against measured (INS) heading.

For each segment, loads a few frames, merges them, and compares
``xopr.add_heading`` output to the recorded ``Heading`` at native rate and
after the 2 s resample used in the QC demo. Reports circular error, rate
agreement, and ``heading_change`` mask agreement at several thresholds.

Usage: uv run python scripts/validate_gps_heading.py [--out DIR] [--frames N]
"""

import argparse
import warnings
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

import xopr
from xopr.qc import heading_change, heading_rate
from xopr.radar_util import add_along_track, add_heading

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# collection -> segment_path (None = pick the segment with most frames among the first 60 items)
SEGMENTS = {
    "2009_Antarctica_DC8": None,
    "2013_Antarctica_P3": None,
    "2014_Greenland_P3": None,
    "2016_Antarctica_DC8": "20161117_06",
    "2018_Greenland_P3": None,
    "2019_Antarctica_GV": "20191105_01",
    "2022_Antarctica_BaslerMKB": "20230109_01",
    "2018_Antarctica_Ground": None,
}
THRESHOLDS = (1.0, 2.0, 5.0)


def wrap(a):
    return (a + np.pi) % (2 * np.pi) - np.pi


def circ_mean_filter(h, window):
    from scipy.ndimage import uniform_filter1d
    ok = np.isfinite(h)
    s = uniform_filter1d(np.where(ok, np.sin(h), 0), window, mode="nearest")
    c = uniform_filter1d(np.where(ok, np.cos(h), 0), window, mode="nearest")
    return np.arctan2(s, c)


def candidate_segments(opr, collection):
    """Segments among the first 60 items, most frames first."""
    items = opr.query_frames(collections=[collection], max_items=60, exclude_geometry=True)
    props = pd.DataFrame(list(items["properties"]))
    items["segment_path"] = (props["opr:date"] + "_" + props["opr:segment"].map("{:02d}".format)).values
    items["frame"] = props["opr:frame"].values
    return [(seg, grp.sort_values("frame")) for seg, grp in
            sorted(items.groupby("segment_path"), key=lambda kv: -len(kv[1]))]


def load_line(opr, collection, seg, n_frames):
    """Load up to n_frames of a segment that has a measured Heading (presence varies by segment)."""
    if seg is not None:
        cands = [(seg, opr.query_frames(collections=[collection], segment_paths=[seg], max_items=n_frames))]
    else:
        cands = candidate_segments(opr, collection)[:4]
    for seg, items in cands:
        frames = opr.load_frames(items.head(n_frames))
        if all("Heading" in f for f in frames):
            return seg, xopr.merge_frames(frames)
        print(f"{collection} {seg}: no measured Heading, trying next segment", flush=True)
    raise ValueError("no segment with measured Heading found")


def resample_2s(ds):
    """Demo-style 2 s resample
    measured heading is resampled by vector mean."""
    h = ds["Heading"]
    out = ds.drop_vars("Heading").resample(slow_time="2s").mean()
    s = np.sin(h).resample(slow_time="2s").mean()
    c = np.cos(h).resample(slow_time="2s").mean()
    out["Heading"] = np.arctan2(s, c)
    out["Heading_naive"] = h.resample(slow_time="2s").mean()  # what the demo did
    return out.dropna("slow_time", subset=["Latitude"])


def compare(ds, label):
    ds = add_along_track(ds)
    gps = add_heading(ds, overwrite=True)["Heading"].values
    meas = ds["Heading"].values
    d = wrap(gps - meas)
    ok = np.isfinite(d)
    step = np.nanmedian(np.diff(ds.along_track.values))
    win = max(int(round(1000 / step)), 1)
    resid = wrap(d - circ_mean_filter(d, win))
    r_meas = heading_rate(ds).values
    r_gps = heading_rate(ds.assign(Heading=("slow_time", gps))).values
    both = np.isfinite(r_meas) & np.isfinite(r_gps)
    row = {
        "case": label,
        "n_traces": len(d),
        "gps_nan_frac": float(np.isnan(gps).mean()),
        "median_abs_deg": float(np.degrees(np.nanmedian(np.abs(d)))),
        "p90_abs_deg": float(np.degrees(np.nanpercentile(np.abs(d[ok]), 90))),
        "resid_std_deg": float(np.degrees(np.nanstd(resid[ok]))),
        "rate_r": float(np.corrcoef(r_meas[both], r_gps[both])[0, 1]),
        "rate_p99_absdiff": float(np.nanpercentile(np.abs(r_meas - r_gps)[both], 99)),
    }
    for thr in THRESHOLDS:
        m = heading_change(ds, max_deg_per_km=thr, source="measured")["qc_heading_change"].values
        g = heading_change(ds, max_deg_per_km=thr, source="gps")["qc_heading_change"].values
        fm, fg = ~m, ~g
        row[f"agree_{thr:g}"] = float((m == g).mean())
        row[f"prec_{thr:g}"] = float((fm & fg).sum() / max(fg.sum(), 1))
        row[f"recall_{thr:g}"] = float((fm & fg).sum() / max(fm.sum(), 1))
        row[f"nflag_meas_{thr:g}"] = int(fm.sum())
        row[f"nflag_gps_{thr:g}"] = int(fg.sum())
    if "Heading_naive" in ds:
        naive = heading_change(ds.assign(Heading=ds["Heading_naive"]), max_deg_per_km=5.0,
                               source="measured")["qc_heading_change"].values
        row["nflag_naive_mean_2"] = int((~naive).sum())
    return row, ds, gps, r_meas, r_gps


def plot(ds, gps, r_meas, r_gps, title, path):
    x = ds.along_track.values / 1000
    fig, ax = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
    ax[0].plot(x, np.degrees(ds["Heading"].values), lw=0.8, label="measured")
    ax[0].plot(x, np.degrees(gps), lw=0.8, label="gps", alpha=0.8)
    ax[0].set_ylabel("heading [deg]")
    ax[0].legend()
    ax[0].set_title(title)
    ax[1].plot(x, np.degrees(wrap(gps - ds["Heading"].values)), lw=0.6)
    ax[1].set_ylabel("gps - measured [deg]")
    ax[2].plot(x, r_meas, lw=0.6, label="measured")
    ax[2].plot(x, r_gps, lw=0.6, label="gps", alpha=0.8)
    for thr in THRESHOLDS:
        ax[2].axhline(thr, color="k", ls=":", lw=0.5)
    ax[2].set_ylabel("rate [deg/km]")
    ax[2].set_yscale("symlog", linthresh=1)
    ax[2].legend()
    ax[2].set_xlabel("along-track [km]")
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="../outputs/gps_heading")
    p.add_argument("--frames", type=int, default=4)
    args = p.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    warnings.filterwarnings("ignore")
    opr = xopr.OPRConnection(cache_dir="radar_cache")
    rows = []
    for collection, seg in SEGMENTS.items():
        try:
            seg, line = load_line(opr, collection, seg, args.frames)
        except Exception as e:  # keep going; report in table
            print(f"{collection}: FAILED {type(e).__name__}: {e}", flush=True)
            rows.append({"collection": collection, "segment": seg, "case": "load_failed"})
            continue
        for label, ds in (("native", line), ("resample_2s", resample_2s(line))):
            row, ds, gps, rm, rg = compare(ds, label)
            row = {"collection": collection, "segment": seg, **row}
            rows.append(row)
            print(f"{collection} {seg} {label}: median={row['median_abs_deg']:.2f} p90={row['p90_abs_deg']:.2f} "
                  f"resid={row['resid_std_deg']:.2f} agree2={row['agree_2']:.4f}", flush=True)
            plot(ds, gps, rm, rg, f"{collection} {seg} ({label})", out / f"{collection}_{seg}_{label}.png")
    df = pd.DataFrame(rows)
    df.to_csv(out / "summary.csv", index=False)
    cols = ["collection", "segment", "case", "n_traces", "gps_nan_frac", "median_abs_deg", "p90_abs_deg",
            "resid_std_deg", "rate_r", "agree_1", "agree_2", "agree_5", "prec_2", "recall_2",
            "nflag_meas_2", "nflag_gps_2", "nflag_naive_mean_2"]
    cols = [c for c in cols if c in df]
    fmt = df[cols].map(lambda v: f"{v:.3f}" if isinstance(v, float) else str(v))
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(r) + " |" for r in fmt.values.tolist()]
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print(df[cols].to_string())


if __name__ == "__main__":
    main()

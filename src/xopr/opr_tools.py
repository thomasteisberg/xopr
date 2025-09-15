import xarray as xr

from typing import Iterable

from xopr.util import merge_dicts_no_conflicts

def merge_frames(frames: Iterable[xr.Dataset]) -> list[xr.Dataset]:
    """
    Merge a set of radar frames into a list of merged xarray Datasets. Frames from the
    same segment (typically a flight) are concatenated along the 'slow_time' dimension.

    Parameters
    ----------
    frames : Iterable[xr.Dataset]
        An iterable of xarray Datasets representing radar frames.

    Returns
    -------
    list[xr.Dataset]
        List of merged xarray Datasets.
    """
    segments = {}

    mergable_keys = ['source_url', 'collection', 'data_product', 'granule']
    
    for frame in frames:
        # Get segment path from frame attributes
        granule = frame.attrs.get('granule')
        date, segment_id, frame_id = granule.split('_')
        segment_path = f"{date}_{segment_id}"

        if not segment_path:
            warnings.warn("Frame missing 'granule' attribute or it was not in the expected format, skipping.", UserWarning)
            continue

        if segment_path not in segments:
            segments[segment_path] = []

        segments[segment_path].append(frame)

    # Merge frames for each segment
    merged_segments = []
    for segment_path, segment_frames in segments.items():
        merged_segment = xr.concat(segment_frames, dim='slow_time', combine_attrs=merge_dicts_no_conflicts).sortby('slow_time')

        for k in mergable_keys:
            if k not in merged_segment.attrs:
                merged_segment.attrs[k] = set([v for v in {f.attrs.get(k) for f in segment_frames} if v is not None])

        merged_segments.append(merged_segment)

    return merged_segments
import xarray as xr

from typing import Iterable, Union
import warnings

from xopr.util import get_ror_display_name, merge_dicts_no_conflicts

def merge_frames(frames: Iterable[xr.Dataset]) -> Union[list[xr.Dataset], xr.Dataset]:
    """
    Merge a set of radar frames into a list of merged xarray Datasets. Frames from the
    same segment (typically a flight) are concatenated along the 'slow_time' dimension.

    Parameters
    ----------
    frames : Iterable[xr.Dataset]
        An iterable of xarray Datasets representing radar frames.

    Returns
    -------
    list[xr.Dataset] or xr.Dataset
        List of merged xarray Datasets or a single merged Dataset if there is only one segment.
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

    if len(merged_segments) == 1:
        return merged_segments[0]
    else:
        return merged_segments

def generate_citation(ds : xr.Dataset) -> str:
        """
        Generate a citation string for the dataset based on its attributes.

        Parameters
        ----------
        ds : xr.Dataset
            The xarray Dataset containing metadata.

        Returns
        -------
        str
            A formatted citation string.
        """

        citation_string = ""
        any_citation_info = False

        citation_string += "== Data Citation ==\n"

        if 'ror' in ds.attrs and ds.attrs['ror']:
            any_citation_info = True
            if isinstance(ds.attrs['ror'], (set, list)):
                institution_name = ', '.join([get_ror_display_name(ror) for ror in ds.attrs['ror']])
            else:
                institution_name = get_ror_display_name(ds.attrs['ror'])

            citation_string += f"This data was collected by {institution_name}.\n"

        if 'doi' in ds.attrs and ds.attrs['doi']:
            any_citation_info = True
            citation_string += f"Please cite the dataset DOI: https://doi.org/{ds.attrs['doi']}\n"

        if 'funder_text' in ds.attrs and ds.attrs['funder_text']:
            any_citation_info = True
            citation_string += f"Please include the following funder acknowledgment:\n{ds.attrs['funder_text']}\n"

        if not any_citation_info:
            citation_string += "No specific citation information was retrieved for this dataset. By default, please cite:\n"
            citation_string += "CReSIS. 2024. REPLACE_WITH_RADAR_NAME Data, Lawrence, Kansas, USA. Digital Media. http://data.cresis.ku.edu/."

        # Add general OPR Toolbox citation
        citation_string += "\n== Processing Citation ==\n"
        citation_string += "Data was processed using the Open Polar Radar (OPR) Toolbox: https://doi.org/10.5281/zenodo.5683959\n"
        citation_string += "Please cite the OPR Toolbox as:\n"
        citation_string += "Open Polar Radar. (2024). opr (Version 3.0.1) [Computer software]. https://gitlab.com/openpolarradar/opr/. https://doi.org/10.5281/zenodo.5683959\n"
        citation_string += "And include the following acknowledgment:\n"
        citation_string += "We acknowledge the use of software from Open Polar Radar generated with support from the University of Kansas, NASA grants 80NSSC20K1242 and 80NSSC21K0753, and NSF grants OPP-2027615, OPP-2019719, OPP-1739003, IIS-1838230, RISE-2126503, RISE-2127606, and RISE-2126468.\n"

        return citation_string if citation_string else "No citation information available for this dataset."
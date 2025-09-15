import xarray as xr

from xopr.xarray_repr_fix.formatting_html import dataset_repr

from xopr.util import get_ror_display_name

@xr.register_dataset_accessor("xopr")
class XoprAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def _repr_html_(self):
        return dataset_repr(self._obj)
    
    def generate_citation(self) -> str:
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

        ds = self._obj

        citation_string = ""
        any_citation_info = False

        citation_string += "== Data Citation ==\n"

        if 'ror' in ds.attrs and ds.attrs['ror']:
            any_citation_info = True
            # TODO: Use get_ror_display_name function to convert ROR ID to institution name
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
import xarray as xr
from xopr.xarray_repr_fix.formatting_html import dataset_repr

@xr.register_dataset_accessor("xopr")
class XoprAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        #self._obj._repr_html_ = self._repr_html_()

    def _repr_html_(self):
        return dataset_repr(self._obj)
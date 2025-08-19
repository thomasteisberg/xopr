from collections.abc import Iterable
import h5py
import scipy.io
import numpy as np

#
# HDF5-format MATLAB files
#

def dereference_h5value(value, h5file):
    if isinstance(value, h5py.Reference):
        return dereference_h5value(h5file[value], h5file=h5file)
    elif isinstance(value, h5py.Group):
        # Pass back to decode_hdf5_matlab_variable to handle groups
        return decode_hdf5_matlab_variable(value, h5file=h5file)
    elif isinstance(value, Iterable):
        v = [dereference_h5value(v, h5file=h5file) for v in value]
        try:
            return np.squeeze(np.array(v))
        except:
            return v
    elif isinstance(value, np.number):
        return value.item()
    else:
        return value

def decode_hdf5_matlab_variable(h5var, skip_variables=False, debug_path="", skip_errors=True, h5file=None):
    """
    Decode a MATLAB variable stored in an HDF5 file.
    This function assumes the variable is stored as a byte string.
    """
    if h5file is None:
        h5file = h5var.file
    matlab_class = h5var.attrs.get('MATLAB_class', None)
    
    if matlab_class and matlab_class == b'cell':
        return dereference_h5value(h5var[:], h5file=h5file)
    elif matlab_class and matlab_class == b'char':
        return h5var[:].astype(dtype=np.uint8).tobytes().decode('utf-8')
    elif isinstance(h5var, (h5py.Group, h5py.File)):
        attrs = {}
        for k in h5var:
            if k.startswith('#'):
                continue
            if 'api_key' in k:
                attrs[k] = "API_KEY_REMOVED"
                continue
            if isinstance(h5var[k], h5py.Dataset):
                if not skip_variables:
                    try:
                        attrs[k] = decode_hdf5_matlab_variable(h5var[k], debug_path=debug_path + "/" + k, skip_errors=skip_errors, h5file=h5file)
                    except Exception as e:
                        print(f"Failed to decode variable {k} at {debug_path}: {e}")
                        if not skip_errors:
                            raise e
            else:
                attrs[k] = decode_hdf5_matlab_variable(h5var[k], debug_path=debug_path + "/" + k, skip_errors=skip_errors, h5file=h5file)
        return attrs
    else:
        return np.squeeze(h5var[:])

#
# Legacy MATLAB files (non-HDF5)
#

def extract_legacy_mat_attributes(file, skip_keys=[], skip_errors=True):
    m = scipy.io.loadmat(file, mat_dtype=True, simplify_cells=True, squeeze_me=True)

    attrs = {}
    for key, value in m.items():
        if key.startswith('__') or key in skip_keys:
            continue
        else:
            attrs[key] = value

    return strip_api_key(attrs)

def strip_api_key(attrs):
    attrs_clean = {}
    for key, value in attrs.items():
        if 'api_key' in key:
            attrs_clean[key] = "API_KEY_REMOVED"
        elif isinstance(value, dict):
            attrs_clean[key] = strip_api_key(value)
        else:
            attrs_clean[key] = value
    return attrs_clean
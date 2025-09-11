# Map display using OpenLayers, arrow, and parquet

This module contains all of the code needed for displaying maps in Polar
Stereographic (North or South) backed by the NASA 'Blue Marble' WMS tile
server, and displaying vector geometries from a geoparquet file source.
Note that since STAC catalogues stored as parquet are also valid
geoparquet files, we use this to display STAC catalogs.

With the exception of the `polar.html` file, most of the files within
this module are not referenced directly. This is because we serve the
`parquet_wasm_bg.wasm`, `parquet_wasm.js`, and
`test_antarctic_random_walk.parquet` files from Google Cloud Storage...
which we do to avoid common and annoying issues such as tracking
relative paths when building documentation pages that incorporate the
polar maps, and hard to debug CORS errors when accessing resources that
are hosted on cloud infrastructure that we don't directly control. To
this point, the `parquet_wasm_bg.wasm` and `parquet_wasm.js` library scripts
are from the external [parquet-wasm
project](https://github.com/kylebarron/parquet-wasm); we include them
(unmodified) here for completeness, and host them on our cloud infrastructure
at https://storage.googleapis.com/opr_test_dataset_1/parquet_wasm.js and
https://storage.googleapis.com/opr_test_dataset_1/parquet_wasm_bg.wasm where we
have configured the buckets with liberal and compliant CORS access policies.

## Map Display

Below is an example of an Antarctic map showing test data. The map loads GeoParquet files directly in the browser using WebAssembly.

```html
<iframe 
    src="./polar.html"  # this is usually set to "../_static/maps/polar.html" in our docs generation code
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = {pole: 'south', parquetFiles: ['https://storage.googleapis.com/opr_test_dataset_1/test_antarctic_random_walk.parquet'], defaultZoom: 3}">
</iframe>
```

## How It Works

1. **GeoParquet Loading**: The map uses `parquet-wasm` to read Parquet files directly in the browser
2. **Projection**: Uses EPSG:3031 for Antarctic Polar Stereographic projection
3. **Basemap**: NASA GIBS Blue Marble imagery via WMS
4. **Interaction**: Click features for details, click empty areas for coordinates

## Data

The example shows:
- A random walk path from the South Pole (LineString)
- Start and end points (Point features)
- Feature properties with names and descriptions

## Configuration

The map can be configured with:
- `pole`: 'north' or 'south' for Arctic/Antarctic
- `parquetFiles`: Array of parquet file paths to load
- `defaultZoom`: Initial zoom level

## Technical Stack

- **OpenLayers** for map rendering and interaction
- **parquet-wasm** for reading Parquet files via WebAssembly
- **Apache Arrow** for parsing Arrow IPC format
- **proj4js** for coordinate transformations

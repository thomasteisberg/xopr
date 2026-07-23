# Map display using OpenLayers, arrow, and parquet

This module contains all of the code needed for displaying maps in Polar
Stereographic (North or South) backed by the NASA 'Blue Marble' WMS tile
server, and displaying vector geometries from a geoparquet file source.
Note that since STAC catalogues stored as parquet are also valid
geoparquet files, we use this to display STAC catalogs.

`polar.html` is self-contained: it loads its JavaScript dependencies
(OpenLayers, proj4js, Apache Arrow, and
[parquet-wasm](https://github.com/kylebarron/parquet-wasm)) from public
CDNs, and reads GeoParquet data from
[source.coop](https://source.coop/englacial/xopr). The docs site serves
it at `/polar.html` via `project.static_files` in `docs/myst.yml`
(works with both `myst start` and `myst build`), where the `polar-map`
MyST directive (`docs/iframe-map.mjs`) embeds it as an iframe.

## Map Display

Below is an example of an Antarctic map showing the CReSIS STAC catalog. The map loads GeoParquet files directly in the browser using WebAssembly; configuration is passed via URL parameters.

```html
<iframe
    src="polar.html?pole=south&dataPath=https://data.source.coop/englacial/xopr/catalog/hemisphere=south&fileGroups=[{"files":["provider=cresis/*"],"color":"navy"}]&defaultZoom=3"
    width="100%"
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;">
</iframe>
```

## How It Works

1. **GeoParquet Loading**: The map uses `parquet-wasm` to read Parquet files directly in the browser
2. **Projection**: Uses EPSG:3031 for Antarctic Polar Stereographic projection
3. **Basemap**: NASA GIBS Blue Marble imagery via WMS
4. **Interaction**: Click features for details, click empty areas for coordinates

## Configuration

The map can be configured with:
- `pole`: 'north' or 'south' for Arctic/Antarctic
- `dataPath`: Base URL for parquet files
- `fileGroups`: JSON array of file groups with colors; file entries may use wildcards
- `parquetFiles`: (legacy) array of parquet file paths to load, single color
- `defaultZoom`: Initial zoom level

## Technical Stack

- **OpenLayers** for map rendering and interaction
- **parquet-wasm** for reading Parquet files via WebAssembly
- **Apache Arrow** for parsing Arrow IPC format
- **proj4js** for coordinate transformations

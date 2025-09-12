# Test Polar Map - Working Version

This page demonstrates the polar map integration with GeoParquet data.

## Map Display

Below is an Antarctic map showing test data. The map loads GeoParquet files directly in the browser using WebAssembly.

:::{polar-map} ../_static/maps/polar.html
:width: 100%
:height: 600px
:pole: south
:parquetFiles: ["https://storage.googleapis.com/opr_test_dataset_1/test_antarctic_random_walk.parquet"]
:defaultZoom: 3
:::

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

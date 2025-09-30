## Map Display

Below is an Antarctic map showing test data. The map loads GeoParquet files directly in the browser using WebAssembly. 

:::{polar-map}
:width: 100%
:height: 600px
:pole: south
:dataPath: https://storage.googleapis.com/opr_stac/catalog
:fileGroups: [{"files": ["provider=cresis/*Antarctica*"], "color": "navy"}, {"files": ["provider=utig/*Antarctica*"], "color": "orange"}, {"files": ["provider=dtu/*Antarctica*"], "color": "red"}, {"files": ["provider=awi/*Antarctica*"], "color": "lightblue"}]
:defaultZoom: 3
:::

:::{polar-map}
:width: 100%
:height: 600px
:pole: north
:dataPath: https://storage.googleapis.com/opr_stac/catalog
:fileGroups: [{"files": ["provider=cresis/*Greenland*"], "color": "navy"}, {"files": ["provider=utig/*Greenland*"], "color": "orange"}, {"files": ["provider=dtu/*Greenland*"], "color": "red"}, {"files": ["provider=awi/*Greenland*"], "color": "lightblue"}]
:defaultZoom: 3
:::

Old syntax still works (uses red by default):
:::{polar-map}
:width: 100%
:height: 600px
:pole: south
:dataPath: https://storage.googleapis.com/opr_test_dataset_1
:parquetFiles: ["test_antarctic_random_walk.parquet"]
:defaultZoom: 3
:::

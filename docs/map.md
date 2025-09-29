## Map Display

Below is an Antarctic map showing test data. The map loads GeoParquet files directly in the browser using WebAssembly. 

 :::{polar-map}
  :width: 100%
  :height: 600px
  :pole: south
  :dataPath: https://storage.googleapis.com/opr_stac/catalog
  :fileGroups: [{"files": ["*2014_Antarctica_DC8.parquet"], "color": "orange"}, {"files": ["*2013_Antarctica_P3.parquet"], "color": "skyblue"}]
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

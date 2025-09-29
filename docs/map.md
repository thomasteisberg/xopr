## Map Display

Below is an Antarctic map showing test data. The map loads GeoParquet files directly in the browser using WebAssembly. 

 :::{polar-map}
  :width: 100%
  :height: 600px
  :pole: south
  :dataPath: https://storage.googleapis.com/opr_test_dataset_1
  :fileGroups: [{"files": ["campaign1_*.parquet"], "color": "orange"}, {"files": ["campaign2_*.parquet"], "color": "skyblue"}]
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

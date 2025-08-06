#!/bin/bash

# Upload OPR STAC catalog to Google Cloud Storage
gsutil cp scripts/output/*.parquet gs://opr_stac/testing/
gsutil cp scripts/output/collections.json gs://opr_stac/testing/collections.json
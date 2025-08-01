#!/bin/bash

# Upload OPR STAC catalog to Google Cloud Storage
gsutil cp scripts/output/opr-stac.parquet gs://opr_stac/testing/opr-stac.parquet
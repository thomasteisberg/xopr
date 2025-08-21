# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Upcoming features and fixes

## [0.1.3] - 2025-08-21

- fixing docs.yml (5433704)
- excluding code that isn't ours from flake8 checks (9f54855)
- fixing imports (6c298b1)
- remove environment.yml and conda based workflows (fca7705)
- changelog workflow and action (3305f85)
- refactored tests and build dependency structure (4e2ddd8)
- updating stac extension in unit test (919c048)
- updating stac extensions to most current version (required for valid stac output from the sar extention) (4b11fb0)
- fixing unit test now that we have collection level geometries (19b85fc)
- dask processing update; fixing issue with hierarchical nesting and conversion to parquet (97a04e3)
- adding in projection extention (required for collection level geometries) (780b042)
- adjust default verbosity (1a65175)
- refactor build_stac_catalog.py, fix imports (2e5df2f)
- actually adding the unit tests this time (9681ecf)
- clean up of stac creation logic (checking and consolidating doi and citations at collection level); first pass unit tests for module (e5114cd)
- Update minimal example and install guide in docs (81ddd28)
- numpydoc style docstrings (88cd84c)
- Change import structure (34a809a)
- Add params notebook to docs index (093d700)
- Add source mime type to dataset attrs (a7cbaee)
- flake back in (b1ca7b5)
- Temporarily disable flake8 in test workflow (5e61834)
- Basic unit tests for data loading (f440cb4)
- Matlab legacy file attributes and nested attribute merging (a9d575e)
- Load all parameters from the HDF5 files (48f4be0)
- Prelimary work on getting the rest of the matlab attributes into xarray datasets (6324b00)
- minor cleanup (58769e1)
- workflow on pull_request (00216df)
- fix for unit test error (5a17502)
- fixing 1-char typo (8327072)
- fixing flake8 error (undefined varible) (9ecc2aa)
- switching to xopr reader for parsing metadata (503e80f)
- numpydoc conversion... (682bd8b)
- updated stac schema (a58ee18)
- default locations (e52886d)
- converted to docstrings to numpydoc format (for sphinx documentation generation) (a3626b9)
- Revert "removing unused environment.yml file; still used for testing" (f5a5180)
- removing unused environment.yml file; binder can use requirements.txt instead (e3832ed)
- require successful tests prior to publishing to pypi (d3902e4)
- moving coverage report into docs build (4fd238a)

- Upcoming features and fixes

## [0.1.0] - (1979-01-01)

- First release

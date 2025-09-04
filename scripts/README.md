# STAC Catalog Generation Workflow

## Overview

The STAC catalog generation now uses a simplified YAML-based configuration workflow:

1. **Configure**: Create/edit a YAML configuration file
2. **Build**: Generate parquet collections (parallel processing per campaign)  
3. **Aggregate**: Create catalog.json from parquet files

## Quick Start

```bash
# 1. Generate parquet collections
python scripts/build_catalog.py --config config/catalog.yaml

# 2. Create catalog.json
python scripts/aggregate_parquet_catalog.py --config config/catalog.yaml
```

## Configuration

All settings are managed through YAML configuration files. See `config/catalog.yaml` for the main template.

### Key Configuration Options

```yaml
data:
  root: "/data/opr"                    # Data location
  primary_product: "CSARP_standard"    # Main product
  campaign_filter: "2016_Antarctica_.*" # Regex filter (optional)
  campaigns:
    include: ["2016_Antarctica_DC8"]   # Explicit list (optional)
    exclude: ["test_campaign"]         # Exclude list (optional)

output:
  path: "./stac_catalog"                # Output directory
  catalog_id: "OPR"                    # Catalog ID
  catalog_description: "Open Polar Radar airborne data"
  license: "various"                   # Data license (e.g., "CC-BY-4.0")

processing:
  n_workers: 4                          # Parallel workers per campaign
  max_items: null                       # Limit items (null = all)
```

## Campaign Filtering

Three ways to filter campaigns:

1. **Regex Pattern**:
   ```bash
   python scripts/build_catalog.py --config config/catalog.yaml \
     data.campaign_filter="2016_Antarctica_.*"
   ```

2. **Include List** (in YAML):
   ```yaml
   data:
     campaigns:
       include: ["2016_Antarctica_DC8", "2017_Antarctica_P3"]
   ```

3. **Exclude List** (in YAML):
   ```yaml
   data:
     campaigns:
       exclude: ["test_campaign", "calibration_flight"]
   ```

## Environments

Use different settings for test/production:

```bash
# Test environment (limited data)
python scripts/build_catalog.py --config config/catalog.yaml --env test

# Production environment (full processing)
python scripts/build_catalog.py --config config/catalog.yaml --env production
```

## Command Line Overrides

Override any configuration option from command line:

```bash
python scripts/build_catalog.py --config config/catalog.yaml \
  processing.n_workers=16 \
  processing.max_items=10 \
  output.path=./my_output
```

## Workflow Examples

### Process Single Campaign

```bash
# Configure for single campaign
python scripts/build_catalog.py --config config/catalog.yaml \
  data.campaign_filter="2016_Antarctica_DC8"

# Create catalog
python scripts/aggregate_parquet_catalog.py --config config/catalog.yaml
```

### Process All 2016 Data

```bash
# Process all 2016 campaigns
python scripts/build_catalog.py --config config/catalog.yaml \
  data.campaign_filter="2016_.*"

# Aggregate
python scripts/aggregate_parquet_catalog.py --config config/catalog.yaml
```

### Test Run

```bash
# Quick test with limited data
python scripts/build_catalog.py --config config/catalog.yaml --env test
```

### Incremental Updates

```bash
# Process new campaign
python scripts/build_catalog.py --config config/catalog.yaml \
  data.campaigns.include='["2024_NewCampaign"]'

# Regenerate catalog.json
python scripts/aggregate_parquet_catalog.py --config config/catalog.yaml
```

## Output Structure

```
stac_catalog/
├── config_used.yaml              # Configuration used
├── 2016_Antarctica_DC8.parquet   # Campaign collection
├── 2017_Antarctica_P3.parquet    # Campaign collection
├── catalog.json                  # Aggregated catalog
└── collections.json              # Collection metadata
```

## Benefits

- **Simpler**: Single YAML file controls everything
- **Reproducible**: Config saved with output
- **Flexible**: Easy filtering and environment switching
- **Efficient**: Parallel processing within campaigns
- **Incremental**: Add new campaigns without reprocessing
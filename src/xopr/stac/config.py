"""
Simple configuration management for STAC catalog generation using OmegaConf.

This module provides minimal configuration loading with OmegaConf,
letting the library handle all the complexity.
"""

import os
from pathlib import Path
from typing import Optional, List, Union
import logging

from omegaconf import OmegaConf, DictConfig


# Register resolvers for common path expansions
OmegaConf.register_new_resolver("pwd", lambda: os.getcwd())
OmegaConf.register_new_resolver("home", lambda: Path.home())
OmegaConf.register_new_resolver("env", lambda x, default="": os.environ.get(x, default))


def load_config(
    config_path: Union[str, Path],
    overrides: Optional[List[str]] = None,
    environment: Optional[str] = None
) -> DictConfig:
    """
    Load configuration from YAML file with optional overrides.
    
    Parameters
    ----------
    config_path : Union[str, Path]
        Path to YAML configuration file
    overrides : List[str], optional
        Command-line overrides in dot notation
        Example: ["data.primary_product=CSARP_qlook", "processing.n_workers=8"]
    environment : str, optional
        Environment name to apply (e.g., "production", "test", "development")
    
    Returns
    -------
    DictConfig
        Configuration object with dot-notation access
        
    Examples
    --------
    >>> conf = load_config("config/catalog.yaml")
    >>> print(conf.data.primary_product)
    'CSARP_standard'
    
    >>> conf = load_config(
    ...     "config/catalog.yaml",
    ...     overrides=["processing.n_workers=16"],
    ...     environment="production"
    ... )
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Load base configuration
    conf = OmegaConf.load(config_path)
    
    # Apply environment-specific overrides if specified
    if environment and "environments" in conf:
        if environment in conf.environments:
            logging.info(f"Applying environment: {environment}")
            env_conf = conf.environments[environment]
            conf = OmegaConf.merge(conf, env_conf)
        else:
            logging.warning(f"Environment '{environment}' not found in config")
    
    # Apply command-line overrides
    if overrides:
        logging.debug(f"Applying overrides: {overrides}")
        override_conf = OmegaConf.from_dotlist(overrides)
        conf = OmegaConf.merge(conf, override_conf)
    
    # Resolve all interpolations (${...} references)
    OmegaConf.resolve(conf)
    
    # Remove environments section from runtime config (no longer needed)
    if "environments" in conf:
        del conf["environments"]
    
    return conf


def save_config(conf: DictConfig, output_path: Union[str, Path], add_metadata: bool = True):
    """
    Save configuration to file for reproducibility.
    
    Parameters
    ----------
    conf : DictConfig
        Configuration to save
    output_path : Union[str, Path]
        Where to save the configuration
    add_metadata : bool
        Whether to add generation metadata
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if add_metadata:
        # Add metadata about when/where this was generated
        from datetime import datetime
        save_conf = OmegaConf.create({
            "_metadata": {
                "generated_at": datetime.now().isoformat(),
                "working_directory": os.getcwd(),
            },
            **OmegaConf.to_container(conf)
        })
    else:
        save_conf = conf
    
    OmegaConf.save(save_conf, output_path)
    logging.info(f"Configuration saved to: {output_path}")


def validate_config(conf: DictConfig) -> bool:
    """
    Basic validation of required configuration fields.
    
    Parameters
    ----------
    conf : DictConfig
        Configuration to validate
        
    Returns
    -------
    bool
        True if valid, raises ValueError if not
    """
    required_fields = [
        "data.root",
        "data.primary_product",
        "output.path",
        "output.catalog_id",
        "output.catalog_description",
    ]
    
    for field in required_fields:
        if OmegaConf.select(conf, field) is None:
            raise ValueError(f"Required configuration field missing: {field}")
    
    # Validate n_workers is positive
    if conf.processing.n_workers <= 0:
        raise ValueError(f"Invalid n_workers: {conf.processing.n_workers}. Must be positive")
    
    # Validate paths exist
    data_root = Path(conf.data.root)
    if not data_root.exists():
        raise ValueError(f"Data root does not exist: {data_root}")
    
    return True


def get_default_config() -> DictConfig:
    """
    Get default configuration as a starting point.
    
    Returns
    -------
    DictConfig
        Default configuration
    """
    return OmegaConf.create({
        "version": "1.0.0",
        "data": {
            "root": "/data/opr",
            "primary_product": "CSARP_standard",
            "extra_products": ["CSARP_layer", "CSARP_qlook"],
            "campaigns": {
                "include": [],
                "exclude": []
            },
            "campaign_filter": ""
        },
        "output": {
            "path": "${pwd}/stac_catalog",
            "catalog_id": "OPR",
            "catalog_description": "Open Polar Radar airborne data",
            "license": "various"
        },
        "processing": {
            "n_workers": 4,
            "memory_limit": "4GB",
            "max_items": None,
        },
        "assets": {
            "base_url": "https://data.cresis.ku.edu/data/rds/",
        },
        "logging": {
            "verbose": False,
            "level": "INFO",
        },
        "geometry": {
            "simplify": True,
            "tolerance": 100.0
        }
    })


def create_config_template(output_path: Union[str, Path]):
    """
    Create a template configuration file with documentation.
    
    Parameters
    ----------
    output_path : Union[str, Path]
        Where to save the template
    """
    template = """# XOPR STAC Catalog Configuration
# =================================
# All paths support environment variables: ${env:HOME}, ${pwd}, etc.

version: "1.0.0"

# Data source and filtering
data:
  root: "/data/opr"  # Root directory with OPR data
  primary_product: "CSARP_standard"  # Main data product
  extra_products:  # Additional products to include if available
    - "CSARP_layer"
    - "CSARP_qlook"
  
  # Campaign filtering (all filters are optional)
  campaigns:
    include: []  # List specific campaigns to process (empty = all)
    exclude: []  # List campaigns to exclude
  campaign_filter: ""  # Regex pattern (e.g., "2016_Antarctica_.*")

# Output and catalog metadata
output:
  path: "${pwd}/stac_catalog"  # Where to save catalog
  catalog_id: "OPR"  # Catalog identifier
  catalog_description: "Open Polar Radar airborne data"  # Catalog description
  license: "various"  # License for the data

# Processing settings
processing:
  n_workers: 4  # Number of workers for parallel processing
  memory_limit: "4GB"  # Memory limit per worker
  max_items: null  # Maximum items to process (null = all)

# Asset URL configuration
assets:
  base_url: "https://data.cresis.ku.edu/data/rds/"

# Logging settings
logging:
  verbose: false
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR

# Geometry processing
geometry:
  simplify: true
  tolerance: 100.0  # Simplification tolerance in meters

# Environment-specific overrides
environments:
  test:
    processing:
      max_items: 10
      n_workers: 2
    output:
      path: "./test_catalog"
    logging:
      verbose: true
  
  production:
    processing:
      n_workers: 16
      memory_limit: "8GB"
    logging:
      level: "WARNING"
"""
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(template)
    print(f"Created configuration template: {output_path}")
    print("Edit this file to customize your settings.")
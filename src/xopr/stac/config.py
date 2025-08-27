"""
Configuration dataclasses for STAC catalog operations.

This module provides configuration objects that encapsulate common parameters
used across catalog building functions, reducing repetition and improving maintainability.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class CatalogConfig:
    """
    Configuration for STAC catalog operations.
    
    This dataclass encapsulates common parameters used across catalog building
    functions, providing a single source of truth for defaults and reducing
    function signature complexity.
    
    Attributes
    ----------
    data_product : str
        Primary data product to process (default: "CSARP_standard")
    extra_data_products : List[str]
        Additional data products to include (default: ['CSARP_layer'])
    base_url : str
        Base URL for asset hrefs (default: "https://data.cresis.ku.edu/data/rds/")
    max_items : Optional[int]
        Maximum number of items to process, None for all items (default: None)
    verbose : bool
        If True, print detailed progress information (default: False)
    n_workers : Optional[int]
        Number of Dask workers for parallel processing, None for non-Dask mode (default: None)
    memory_limit : Optional[str]
        Memory limit per Dask worker, only used if n_workers is set (default: 'auto')
    threads_per_worker : Optional[int]
        Number of threads per Dask worker, only used if n_workers is set (default: 2)
    
    Examples
    --------
    >>> # Use all defaults for sequential processing
    >>> config = CatalogConfig()
    
    >>> # Enable Dask parallel processing
    >>> config = CatalogConfig(n_workers=4, memory_limit='8GB')
    
    >>> # Customize specific options
    >>> config = CatalogConfig(verbose=True, max_items=100)
    
    >>> # Use with catalog functions
    >>> from xopr.stac import build_flat_collection
    >>> collection = build_flat_collection(campaign, data_root, config=config)
    """
    
    # Core catalog parameters
    data_product: str = "CSARP_standard"
    extra_data_products: List[str] = field(default_factory=lambda: ['CSARP_layer'])
    base_url: str = "https://data.cresis.ku.edu/data/rds/"
    max_items: Optional[int] = None
    verbose: bool = False
    
    # Dask parallel processing parameters (None = sequential processing)
    n_workers: Optional[int] = None
    memory_limit: Optional[str] = 'auto'
    threads_per_worker: Optional[int] = 2
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Validate core parameters
        if self.max_items is not None and self.max_items <= 0:
            raise ValueError(f"max_items must be positive, got {self.max_items}")
        
        if not self.base_url.startswith(('http://', 'https://')):
            raise ValueError(f"base_url must be a valid HTTP(S) URL, got {self.base_url}")
        
        # Ensure base_url ends with slash for proper joining
        if not self.base_url.endswith('/'):
            self.base_url += '/'
        
        # Validate Dask parameters if n_workers is set
        if self.n_workers is not None:
            if self.n_workers <= 0:
                raise ValueError(f"n_workers must be positive, got {self.n_workers}")
            
            if self.threads_per_worker is not None and self.threads_per_worker <= 0:
                raise ValueError(f"threads_per_worker must be positive, got {self.threads_per_worker}")
    
    @property
    def use_dask(self) -> bool:
        """Check if Dask parallel processing is enabled."""
        return self.n_workers is not None
    
    def copy_with(self, **kwargs) -> 'CatalogConfig':
        """
        Create a copy of this config with specified fields overridden.
        
        This is useful for creating variations of a base configuration.
        
        Parameters
        ----------
        **kwargs
            Fields to override in the copy
            
        Returns
        -------
        CatalogConfig
            New configuration object with overridden values
            
        Examples
        --------
        >>> base_config = CatalogConfig(verbose=True)
        >>> test_config = base_config.copy_with(max_items=10)
        >>> parallel_config = base_config.copy_with(n_workers=4)
        """
        import copy
        config_dict = copy.deepcopy(self.__dict__)
        config_dict.update(kwargs)
        return CatalogConfig(**config_dict)
    
    def for_dask(self, n_workers: int = 4, memory_limit: str = 'auto', 
                 threads_per_worker: int = 2) -> 'CatalogConfig':
        """
        Create a copy configured for Dask parallel processing.
        
        This is a convenience method for enabling Dask processing.
        
        Parameters
        ----------
        n_workers : int
            Number of Dask workers (default: 4)
        memory_limit : str
            Memory limit per worker (default: 'auto')
        threads_per_worker : int
            Number of threads per worker (default: 2)
            
        Returns
        -------
        CatalogConfig
            New configuration with Dask enabled
            
        Examples
        --------
        >>> config = CatalogConfig(verbose=True)
        >>> parallel_config = config.for_dask(n_workers=8)
        """
        return self.copy_with(
            n_workers=n_workers,
            memory_limit=memory_limit,
            threads_per_worker=threads_per_worker
        )


def config_from_kwargs(config: Optional[CatalogConfig] = None, **kwargs) -> CatalogConfig:
    """
    Create or update a CatalogConfig from keyword arguments.
    
    This helper function supports backward compatibility by allowing functions
    to accept both a config object and individual parameters.
    
    Parameters
    ----------
    config : CatalogConfig, optional
        Existing configuration to use as base
    **kwargs
        Individual configuration parameters to override
        
    Returns
    -------
    CatalogConfig
        Configuration object with applied overrides
        
    Examples
    --------
    >>> # Called with old-style parameters
    >>> config = config_from_kwargs(None, verbose=True, max_items=100)
    
    >>> # Called with config object and overrides
    >>> base = CatalogConfig()
    >>> config = config_from_kwargs(base, verbose=True)
    
    >>> # Called with Dask parameters
    >>> config = config_from_kwargs(None, n_workers=4, verbose=True)
    """
    if config is None:
        # Create new config from kwargs
        valid_fields = {
            'data_product', 'extra_data_products', 'base_url', 
            'max_items', 'verbose', 'n_workers', 'memory_limit', 
            'threads_per_worker'
        }
        config_kwargs = {k: v for k, v in kwargs.items() if k in valid_fields}
        return CatalogConfig(**config_kwargs)
    elif kwargs:
        # Create copy with overrides
        valid_fields = {
            'data_product', 'extra_data_products', 'base_url', 
            'max_items', 'verbose', 'n_workers', 'memory_limit', 
            'threads_per_worker'
        }
        override_kwargs = {k: v for k, v in kwargs.items() if k in valid_fields}
        return config.copy_with(**override_kwargs)
    else:
        # Return as-is
        return config
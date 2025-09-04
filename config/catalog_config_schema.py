"""
YAML Configuration Schema for STAC Catalog Generation.

This module defines the Cerberus validation schema for catalog configuration files.
It ensures configuration files are valid and provides helpful error messages.
"""

# Cerberus schema definition
CATALOG_CONFIG_SCHEMA = {
    'version': {
        'type': 'string',
        'required': True,
        'regex': r'^\d+\.\d+\.\d+$',
        'meta': 'Configuration schema version (e.g., "1.0.0")'
    },
    
    'data': {
        'type': 'dict',
        'required': True,
        'schema': {
            'root': {
                'type': 'string',
                'required': True,
                'meta': 'Root directory containing OPR data'
            },
            'primary_product': {
                'type': 'string',
                'required': True,
                'allowed': ['CSARP_standard', 'CSARP_qlook', 'CSARP_mvdr', 'CSARP_music', 'CSARP_layer'],
                'meta': 'Primary data product to process'
            },
            'extra_products': {
                'type': 'list',
                'nullable': True,
                'schema': {
                    'type': 'string',
                    'regex': r'^CSARP_\w+$'
                },
                'meta': 'Additional data products to include'
            },
            'campaigns': {
                'type': 'dict',
                'schema': {
                    'include': {
                        'type': 'list',
                        'schema': {'type': 'string'},
                        'meta': 'Campaign names to include'
                    },
                    'exclude': {
                        'type': 'list',
                        'schema': {'type': 'string'},
                        'meta': 'Campaign names to exclude'
                    }
                }
            },
            'flights': {
                'type': 'dict',
                'schema': {
                    'include': {
                        'type': 'list',
                        'schema': {'type': 'string'},
                        'meta': 'Flight patterns to include'
                    },
                    'exclude': {
                        'type': 'list',
                        'schema': {'type': 'string'},
                        'meta': 'Flight patterns to exclude'
                    }
                }
            }
        }
    },
    
    'output': {
        'type': 'dict',
        'required': True,
        'schema': {
            'path': {
                'type': 'string',
                'required': True,
                'meta': 'Output directory path'
            },
            'catalog_id': {
                'type': 'string',
                'required': True,
                'regex': r'^[a-z0-9\-]+$',
                'meta': 'Catalog identifier (lowercase alphanumeric and hyphens)'
            },
            'formats': {
                'type': 'dict',
                'schema': {
                    'stac_json': {'type': 'boolean'},
                    'geoparquet': {'type': 'boolean'},
                    'flat_parquet': {'type': 'boolean'},
                    'collections_metadata': {'type': 'boolean'}
                }
            },
            'organization': {
                'type': 'dict',
                'schema': {
                    'structure': {
                        'type': 'string',
                        'allowed': ['flat', 'hierarchical']
                    },
                    'grouping': {
                        'type': 'string',
                        'allowed': ['campaign', 'year', 'none']
                    },
                    'max_items_per_catalog': {
                        'type': 'integer',
                        'min': 1,
                        'nullable': True
                    }
                }
            }
        }
    },
    
    'assets': {
        'type': 'dict',
        'schema': {
            'base_url': {
                'type': 'string',
                'required': True,
                'meta': 'Base URL for assets or "relative"'
            },
            'naming': {
                'type': 'dict',
                'schema': {
                    'use_product_name': {'type': 'boolean'},
                    'include_data_alias': {'type': 'boolean'}
                }
            },
            'validation': {
                'type': 'dict',
                'schema': {
                    'check_existence': {'type': 'boolean'},
                    'include_metadata': {'type': 'boolean'}
                }
            }
        }
    },
    
    'processing': {
        'type': 'dict',
        'required': True,
        'schema': {
            'mode': {
                'type': 'string',
                'required': True,
                'allowed': ['sequential', 'parallel', 'dask'],
                'meta': 'Processing mode'
            },
            'parallel': {
                'type': 'dict',
                'schema': {
                    'n_workers': {
                        'type': 'integer',
                        'min': 1,
                        'max': 128
                    },
                    'memory_limit': {
                        'type': 'string',
                        'regex': r'^\d+(\.\d+)?(GB|MB|KB|B)?$|^auto$'
                    },
                    'threads_per_worker': {
                        'type': 'integer',
                        'min': 1,
                        'max': 16
                    },
                    'scheduler_address': {
                        'type': 'string',
                        'nullable': True
                    },
                    'chunk_size': {
                        'type': 'integer',
                        'min': 1
                    }
                }
            },
            'limits': {
                'type': 'dict',
                'schema': {
                    'max_items': {
                        'type': 'integer',
                        'min': 1,
                        'nullable': True
                    },
                    'max_flights_per_campaign': {
                        'type': 'integer',
                        'min': 1,
                        'nullable': True
                    },
                    'item_timeout': {
                        'type': 'integer',
                        'min': 1
                    }
                }
            },
            'errors': {
                'type': 'dict',
                'schema': {
                    'continue_on_error': {'type': 'boolean'},
                    'max_consecutive_errors': {
                        'type': 'integer',
                        'min': 0
                    },
                    'save_error_log': {'type': 'boolean'}
                }
            }
        }
    },
    
    'metadata': {
        'type': 'dict',
        'schema': {
            'geometry': {
                'type': 'dict',
                'schema': {
                    'simplify': {'type': 'boolean'},
                    'simplification_tolerance': {
                        'type': 'float',
                        'min': 0.0
                    },
                    'use_polar_projection': {'type': 'boolean'},
                    'polar_epsg': {
                        'type': 'integer',
                        'allowed': [3031, 3413, 3857, 4326]
                    }
                }
            },
            'scientific': {
                'type': 'dict',
                'schema': {
                    'include_sar_metadata': {'type': 'boolean'},
                    'include_citations': {'type': 'boolean'},
                    'consolidate_dois': {'type': 'boolean'}
                }
            },
            'stac_extensions': {
                'type': 'list',
                'schema': {
                    'type': 'string',
                    'regex': r'^https?://.*\.json$'
                }
            }
        }
    },
    
    'logging': {
        'type': 'dict',
        'schema': {
            'level': {
                'type': 'string',
                'allowed': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            },
            'verbose': {'type': 'boolean'},
            'log_to_file': {'type': 'boolean'},
            'log_file': {'type': 'string'},
            'progress': {
                'type': 'dict',
                'schema': {
                    'show_progress_bar': {'type': 'boolean'},
                    'update_interval': {
                        'type': 'float',
                        'min': 0.1
                    },
                    'show_statistics': {'type': 'boolean'}
                }
            }
        }
    },
    
    'validation': {
        'type': 'dict',
        'schema': {
            'validate_items': {'type': 'boolean'},
            'stac_version': {
                'type': 'string',
                'regex': r'^\d+\.\d+\.\d+$'
            },
            'checks': {
                'type': 'dict',
                'schema': {
                    'required_properties': {'type': 'boolean'},
                    'validate_geometry': {'type': 'boolean'},
                    'validate_asset_urls': {'type': 'boolean'},
                    'check_duplicates': {'type': 'boolean'}
                }
            }
        }
    },
    
    'cache': {
        'type': 'dict',
        'schema': {
            'enabled': {'type': 'boolean'},
            'directory': {'type': 'string'},
            'expiration': {
                'type': 'integer',
                'min': 0
            },
            'max_size': {
                'type': 'number',
                'min': 0
            }
        }
    },
    
    'advanced': {
        'type': 'dict',
        'schema': {
            'discovery': {
                'type': 'dict',
                'schema': {
                    'mat_file_pattern': {'type': 'string'},
                    'exclude_patterns': {
                        'type': 'list',
                        'schema': {'type': 'string'}
                    }
                }
            },
            'performance': {
                'type': 'dict',
                'schema': {
                    'batch_size': {
                        'type': 'integer',
                        'min': 1
                    },
                    'connection_pool_size': {
                        'type': 'integer',
                        'min': 1
                    },
                    'request_timeout': {
                        'type': 'integer',
                        'min': 1
                    }
                }
            },
            'compatibility': {
                'type': 'dict',
                'schema': {
                    'support_legacy': {'type': 'boolean'},
                    'strict_mode': {'type': 'boolean'}
                }
            }
        }
    },
    
    'environments': {
        'type': 'dict',
        'keysrules': {
            'type': 'string',
            'regex': r'^[a-z]+$'
        },
        'valuesrules': {
            'type': 'dict'
        },
        'meta': 'Environment-specific configuration overrides'
    }
}


def get_schema_documentation():
    """
    Generate human-readable documentation from the schema.
    
    Returns
    -------
    str
        Markdown-formatted documentation of the configuration schema
    """
    docs = ["# Catalog Configuration Schema\n\n"]
    docs.append("## Required Fields\n\n")
    
    def extract_required(schema, prefix=""):
        required = []
        for key, rules in schema.items():
            if isinstance(rules, dict):
                path = f"{prefix}.{key}" if prefix else key
                if rules.get('required'):
                    meta = rules.get('meta', 'No description')
                    required.append(f"- `{path}`: {meta}")
                if rules.get('type') == 'dict' and 'schema' in rules:
                    required.extend(extract_required(rules['schema'], path))
        return required
    
    required_fields = extract_required(CATALOG_CONFIG_SCHEMA)
    docs.extend(required_fields)
    
    docs.append("\n\n## Field Types and Constraints\n\n")
    
    def document_field(key, rules, indent=0):
        lines = []
        prefix = "  " * indent
        
        if 'meta' in rules:
            lines.append(f"{prefix}- **{key}**: {rules['meta']}")
        else:
            lines.append(f"{prefix}- **{key}**")
        
        if 'type' in rules:
            lines.append(f"{prefix}  - Type: `{rules['type']}`")
        
        if 'allowed' in rules:
            lines.append(f"{prefix}  - Allowed values: {', '.join(map(str, rules['allowed']))}")
        
        if 'regex' in rules:
            lines.append(f"{prefix}  - Pattern: `{rules['regex']}`")
        
        if 'min' in rules or 'max' in rules:
            constraints = []
            if 'min' in rules:
                constraints.append(f"min: {rules['min']}")
            if 'max' in rules:
                constraints.append(f"max: {rules['max']}")
            lines.append(f"{prefix}  - Constraints: {', '.join(constraints)}")
        
        if rules.get('nullable'):
            lines.append(f"{prefix}  - Nullable: yes")
        
        if rules.get('type') == 'dict' and 'schema' in rules:
            for subkey, subrules in rules['schema'].items():
                lines.extend(document_field(subkey, subrules, indent + 1))
        
        return lines
    
    for key, rules in CATALOG_CONFIG_SCHEMA.items():
        docs.extend(document_field(key, rules))
        docs.append("")
    
    return "\n".join(docs)
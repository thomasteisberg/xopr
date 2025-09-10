# xopr

## Overview

xopr is a Python library designed to make accessing [Open Polar Radar's](https://ops.cresis.ku.edu/) data archives easy, scalable, and reproducible.

## Current Antarctic data accessible by xopr:  

<script>
    // Configuration using MyST substitutions
    window.ANTARCTICA_MAP_CONFIG = {
        pole: 'south',
        parquetFiles: ['https://storage.googleapis.com/opr_stac/testing/2010_Antarctica_DC8.parquet','https://storage.googleapis.com/opr_stac/testing/2011_Antarctica_DC8.parquet'],
        defaultZoom: 3
    };
</script>
<iframe 
    src="./_static/maps/polar.html" 
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = window.ANTARCTICA_MAP_CONFIG">
</iframe>

## Current Arctic data accessible by xopr:  

<script>
    // Configuration using MyST substitutions
    window.ARCTIC_MAP_CONFIG = {
        pole: 'north',
        parquetFiles: ['https://storage.googleapis.com/opr_stac/testing/2011_Greenland_P3.parquet'],
        defaultZoom: 3
    };
</script>
<iframe 
    src="./_static/maps/polar.html" 
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = window.ARCTIC_MAP_CONFIG">
</iframe>

:::{warning}
xopr is a work in progress! The API will almost certainly change in the future, so please proceed with caution.

We welcome your feedback and contributions. If you run into problems or have ideas for how this could be better, please consider [opening an issue](https://github.com/thomasteisberg/xopr/issues/new/choose). We also welcome pull requests!
:::

## Installing xopr

For now, xopr is available only directly from source on GitHub. To install xopr, use:

:::{code}
pip install xopr
:::

Or, using [uv](https://docs.astral.sh/uv/):

:::{code}
uv add xopr
:::

## Getting Started

Minimal example of loading and plotting a single frame of radar data:

```python
import numpy as np
import xopr

opr = xopr.OPRConnection()

frames = opr.load_flight("2022_Antarctica_BaslerMKB", flight_id="20221228_01", data_product="CSARP_standard", max_items=1)

(10*np.log10(frames[0].Data)).plot.imshow(x='slow_time', y='twtt', cmap='gray', yincrease=False)
```

To learn more, check out our demo notebooks from the menu on the left side or [on GitHub](https://github.com/thomasteisberg/xopr/tree/thomas/uv-migration/docs/notebooks).

## Design

For details on the initial design planning of xopr, please see [this OPR wiki page](https://gitlab.com/openpolarradar/opr/-/wikis/OPR-Data-Access-Tool-Planning).

:::{figure} img/opr-data-access-infra-1.png
:align: center
:width: 80%

xopr acts as an interface to OPR data. It has two primary roles: helping create queries to the OPR STAC catalog to find data and returning radar data in the form of an xarray Dataset.
:::


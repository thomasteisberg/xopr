# xOPR

## Overview

xOPR is a Python library for working with data products from [Open Polar Radar's](https://ops.cresis.ku.edu/) data archives. xOPR is designed make working with radar sounder data easy, scalable, and reproducible.

:::{tip}
We welcome your feedback and contributions. If you run into problems or have ideas for how this could be better, please consider [opening an issue](https://github.com/englacial/xopr/issues/new/choose). We also welcome pull requests!
:::

xOPR offers access to most of the OPR data catalog, but not absolutely every line. Check out our [availability maps](map.md) for details.

## Installing xOPR

To install xOPR, use:

:::{code}
pip install xopr
:::

Or, using [uv](https://docs.astral.sh/uv/) (our recommendation!):

:::{code}
uv add xopr
:::

## Getting Started

Minimal example of loading and plotting a single frame of radar data:

```python
import numpy as np
import xopr

opr = xopr.OPRConnection()

stac_items = opr.query_frames(collections=["2022_Antarctica_BaslerMKB"], segment_paths=["20221228_01"], max_items=1)
frames = opr.load_frames(stac_items)

(10*np.log10(frames[0].Data)).plot.imshow(x='slow_time', y='twtt', cmap='gray', yincrease=False)
```

For a more complete getting started guide, see [getting started](getting-started.md). Or jump in by checking out some of the notebook on the left.

## Design

For current design and terminology, see the [design notes](design.md) page.

## API Reference

Full API documentation for all xopr modules is available in the [API Reference](api/xopr.html).

:::{figure} img/opr-data-access-infra-1.png
:align: center
:width: 80%

xOPR acts as an interface to OPR data. It has two primary roles: helping create queries to the OPR STAC catalog to find data and returning radar data in the form of an Xarray Dataset.
:::

## About

xOPR was originally written by Shane Grigsby and Thomas Teisberg, with extensive input from John Paden. The ongoing development of xOPR is supported by the [Astera Institute](https://astera.org/) as a project of [Englacial](https://englacial.org/).

The current maintainer is Thomas. You can reach him at thomas.teisberg@astera.org.
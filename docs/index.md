# xopr

## Overview

xopr is a Python library designed to make accessing [Open Polar Radar's](https://ops.cresis.ku.edu/) data archives easy, scalable, and reproducible.

:::{warning}
xopr is a work in progress! The API will almost certainly change in the future, so please proceed with caution.

We welcome your feedback and contributions. If you run into problems or have ideas for how this could be better, please consider [opening an issue](https://github.com/thomasteisberg/xopr/issues/new/choose). We also welcome pull requests!
:::

## Installing xopr

For now, xopr is available only directly from source on GitHub. To install xopr, use:

:::{code}
pip install git+https://github.com/thomasteisberg/xopr
:::

Or, using [uv](https://docs.astral.sh/uv/):

:::{code}
uv add "xopr @ git+https://docs.astral.sh/uv/"
:::

## Getting Started

The best way to get started is to check out some of our demo notebooks from the menu on the left side or [on GitHub](https://github.com/thomasteisberg/xopr/tree/thomas/uv-migration/docs/notebooks).

## Design

For details on the initial design planning of xopr, please see [this OPR wiki page](https://gitlab.com/openpolarradar/opr/-/wikis/OPR-Data-Access-Tool-Planning).

:::{figure} img/opr-data-access-infra-1.png
:align: center
:width: 80%

xopr acts as an interface to OPR data. It has two primary roles: helping create queries to the OPR STAC catalog to find data and returning radar data in the form of an xarray Dataset.
:::


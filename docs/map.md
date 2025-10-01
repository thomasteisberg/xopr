## Data availability

Below are Antarctic and Arctic maps showing radar data availability. These maps loads GeoParquet files (STAC catalogs of radar flight lines) directly in the browser using WebAssembly; they update in real time as any additional catalogs are added.

**Legend**
  - [CReSIS](https://cresis.ku.edu/) lines are Navy 
  - [UTIG](https://ig.utexas.edu/) lines are Orange
  - [AWI](https://www.awi.de/en/science/geosciences/glaciology/tools/radar.html) lines are Light Blue
  - [BAS](https://www.bas.ac.uk/polar-operations/sites-and-facilities/facility/phase-sensitive-radar-apres/) lines are Black
  - [DTU](https://www.space.dtu.dk/english/research-divisions/microwaves-and-remote-sensing/research-areas/radar-systems) lines are Red
  - [Columbia](https://lamont.columbia.edu/research-divisions/marine-polar-geophysics) lines are Yellow
  - [UW](https://environment.uw.edu/news/2020/02/new-radar-technology-sheds-light-on-never-before-seen-antarctic-landscape/) lines are Purple

### Antarctica

:::{polar-map}
:width: 100%
:height: 600px
:pole: south
:dataPath: https://storage.googleapis.com/opr_stac/catalog/hemisphere=south
:fileGroups: [{"files": ["provider=cresis/*"], "color": "navy"}, {"files": ["provider=utig/*"], "color": "orange"}]
:defaultZoom: 3
:::

*Note, the 2018 UTIG data currently have only been processed to CSARP_qlook; processing to CSARP_standard is pending*

### Greenland

:::{polar-map}
:width: 100%
:height: 600px
:pole: north
:dataPath: https://storage.googleapis.com/opr_stac/catalog/hemisphere=north
:fileGroups: [{"files": ["provider=cresis/*"], "color": "navy"}, {"files": ["provider=utig/*"], "color": "orange"}, {"files": ["provider=dtu/*"], "color": "red"}, {"files": ["provider=awi/*"], "color": "lightblue"}]
:defaultZoom: 3
:::


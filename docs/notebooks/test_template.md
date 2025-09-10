# Test Template for Polar Maps

This page tests MyST templating for embedding polar maps with dynamic configuration.

```{eval-rst}
.. raw:: html

   <script>
   // Define configuration inline at build time
   window.POLAR_CONFIG_1 = {
       pole: 'south',
       parquetFiles: [
           'https://storage.googleapis.com/opr_stac/testing/2010_Antarctica_DC8.parquet',
           'https://storage.googleapis.com/opr_stac/testing/2011_Antarctica_DC8.parquet'
       ],
       defaultZoom: 3
   };
   </script>
   
   <iframe 
       id="polar-map-1"
       src="../_static/maps/polar.html" 
       width="100%" 
       height="600"
       frameborder="0"
       style="border: 1px solid #ccc; border-radius: 5px;"
       onload="this.contentWindow.CONFIG = window.POLAR_CONFIG_1">
   </iframe>
```

## Alternative: Using MyST Substitutions

```{code-block} javascript
---
substitutions:
  antarctica_files: |
    ['https://storage.googleapis.com/opr_stac/testing/2010_Antarctica_DC8.parquet',
     'https://storage.googleapis.com/opr_stac/testing/2011_Antarctica_DC8.parquet']
  greenland_files: |
    ['https://storage.googleapis.com/opr_stac/testing/2011_Greenland_P3.parquet',
     'https://storage.googleapis.com/opr_stac/testing/2012_Greenland_P3.parquet']
---
```

## Option 3: Direct HTML with Embedded Script

Let's try embedding the configuration directly in a script tag that runs before the iframe loads:

<div id="map-container-1">
<script>
    // Store config in a global variable specific to this map
    window.ANTARCTICA_CONFIG = {
        pole: 'south',
        parquetFiles: [
            'https://storage.googleapis.com/opr_stac/testing/2010_Antarctica_DC8.parquet',
            'https://storage.googleapis.com/opr_stac/testing/2011_Antarctica_DC8.parquet'
        ],
        defaultZoom: 3
    };
</script>
<iframe 
    src="../_static/maps/polar.html" 
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = window.ANTARCTICA_CONFIG">
</iframe>
</div>

## Option 4: Using HTML literals

<script>
    window.GREENLAND_CONFIG = {
        pole: 'north',
        parquetFiles: [
            'https://storage.googleapis.com/opr_stac/testing/2011_Greenland_P3.parquet',
            'https://storage.googleapis.com/opr_stac/testing/2012_Greenland_P3.parquet'
        ],
        defaultZoom: 3
    };
</script>
<iframe 
    src="../_static/maps/polar.html" 
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = window.GREENLAND_CONFIG">
</iframe>

## Option 5: Creating a custom directive

We could also create a custom MyST directive that generates the proper HTML at build time. This would look like:

````
```{polar-map}
:pole: north
:parquet-files: https://storage.googleapis.com/opr_stac/testing/2011_Greenland_P3.parquet,https://storage.googleapis.com/opr_stac/testing/2012_Greenland_P3.parquet
:height: 600
```
````

But this would require adding Python code to handle the directive.

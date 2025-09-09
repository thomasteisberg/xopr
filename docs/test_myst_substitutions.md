---
substitutions:
  antarctica_pole: south
  antarctica_files: |
    ['https://storage.googleapis.com/opr_stac/testing/2010_Antarctica_DC8.parquet',
     'https://storage.googleapis.com/opr_stac/testing/2011_Antarctica_DC8.parquet']
  greenland_pole: north
  greenland_files: |
    ['https://storage.googleapis.com/opr_stac/testing/2011_Greenland_P3.parquet',
     'https://storage.googleapis.com/opr_stac/testing/2012_Greenland_P3.parquet']
---

# Test MyST Native Substitutions for Polar Maps

This tests MyST's native variable substitution feature for embedding polar maps.

## Method 1: Using Substitutions in HTML Blocks

```{raw} html
<script>
    // Define configuration using MyST substitutions
    window.ANTARCTICA_CONFIG = {
        pole: '{{ antarctica_pole }}',
        parquetFiles: {{ antarctica_files }},
        defaultZoom: 3
    };
</script>
<iframe 
    src="./_static/maps/polar.html" 
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = window.ANTARCTICA_CONFIG">
</iframe>
```

## Method 2: Direct Substitution in onload

```{raw} html
<iframe 
    src="./_static/maps/polar.html" 
    width="100%" 
    height="600"
    frameborder="0"
    style="border: 1px solid #ccc; border-radius: 5px;"
    onload="this.contentWindow.CONFIG = {pole: '{{ greenland_pole }}', parquetFiles: {{ greenland_files }}, defaultZoom: 3}">
</iframe>
```

## Method 3: Using eval-rst for More Control

```{eval-rst}
.. raw:: html

   <script>
   window.GREENLAND_CONFIG_2 = {
       pole: '{{ greenland_pole }}',
       parquetFiles: {{ greenland_files }},
       defaultZoom: 3
   };
   </script>
   <iframe 
       src="./_static/maps/polar.html" 
       width="100%" 
       height="600"
       frameborder="0"
       style="border: 1px solid #ccc;"
       onload="this.contentWindow.CONFIG = window.GREENLAND_CONFIG_2">
   </iframe>
```

## Testing Variable Substitution

The pole for Antarctica is: {{ antarctica_pole }}

The pole for Greenland is: {{ greenland_pole }}

This should show that MyST substitutions work inline as well!
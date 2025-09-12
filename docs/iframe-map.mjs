const polarMapDirective = {
  name: 'polar-map',
  doc: 'Embed an interactive polar map with GeoParquet data configuration',
  arg: {
    type: String,
    doc: 'The iframe source URL for the polar map',
    required: true
  },
  options: {
    // Standard iframe options
    width: { 
      type: String, 
      doc: 'Width in CSS units (e.g., 100%, 600px)' 
    },
    height: { 
      type: String, 
      doc: 'Height in CSS units (e.g., 600px, 80vh)' 
    },
    align: { 
      type: String, 
      doc: 'Alignment of the map: left, center, or right',
      values: ['left', 'center', 'right']
    },
    class: { 
      type: String, 
      doc: 'Space-delimited CSS class names' 
    },
    
    // Custom polar map configuration options
    pole: { 
      type: String, 
      doc: 'Which pole to display: north or south',
      values: ['north', 'south']
    },
    parquetFiles: { 
      type: String,  // JSON string that will be parsed
      doc: 'JSON array of parquet file URLs to load (e.g., ["url1", "url2"])'
    },
    defaultZoom: { 
      type: Number,
      doc: 'Initial zoom level for the map'
    }
  },
  run(data) {
    const { arg: src, options = {} } = data;
    
    // Build configuration object from custom options
    const config = {};
    if (options.pole) {
      config.pole = options.pole;
    }
    if (options.parquetFiles) {
      try {
        // Parse the JSON string to get the array
        config.parquetFiles = JSON.parse(options.parquetFiles.replace(/'/g, '"'));
      } catch (e) {
        console.warn('Failed to parse parquetFiles, using as string:', options.parquetFiles);
        config.parquetFiles = [options.parquetFiles];
      }
    }
    if (options.defaultZoom !== undefined) {
      config.defaultZoom = options.defaultZoom;
    }
    
    // Create the iframe node with standard attributes
    const iframeNode = {
      type: 'iframe',
      src,
      width: options.width || '100%',
      height: options.height || '600px',
      frameborder: '0',
      style: 'border: 1px solid #ccc; border-radius: 5px;'
    };
    
    // Add alignment if specified
    if (options.align) {
      iframeNode.align = options.align;
    }
    
    // Add CSS classes if specified
    if (options.class) {
      iframeNode.class = options.class;
    }
    
    // If we have config options, create a container with script injection
    if (Object.keys(config).length > 0) {
      // Generate a unique ID for this iframe
      const iframeId = `polar-map-${Math.random().toString(36).substr(2, 9)}`;
      iframeNode.id = iframeId;
      
      return [{
        type: 'container',
        kind: 'polar-map-container',
        children: [
          iframeNode,
          {
            type: 'html',
            value: `<script>
              (function() {
                const iframe = document.getElementById('${iframeId}');
                if (iframe) {
                  iframe.onload = function() {
                    this.contentWindow.CONFIG = ${JSON.stringify(config, null, 2)};
                  };
                }
              })();
            </script>`
          }
        ]
      }];
    }
    
    // Return just the iframe if no config
    return [iframeNode];
  }
};

const plugin = {
  name: 'Polar Map iframe plugin',
  directives: [polarMapDirective]
};

export default plugin;
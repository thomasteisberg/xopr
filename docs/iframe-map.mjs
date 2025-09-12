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
      doc: 'JSON array of parquet filenames (e.g., ["file1.parquet", "file2.parquet"])'
    },
    dataPath: {
      type: String,
      doc: 'Base URL path for parquet files (e.g., "https://storage.googleapis.com/bucket/")'
    },
    defaultZoom: { 
      type: Number,
      doc: 'Initial zoom level for the map'
    }
  },
  run(data) {
    const { arg: src, options = {} } = data;
    
    // Build URL parameters for configuration
    const params = new URLSearchParams();
    
    if (options.pole) {
      params.set('pole', options.pole);
    }
    
    if (options.dataPath) {
      params.set('dataPath', options.dataPath);
    }
    
    if (options.parquetFiles) {
      try {
        // Parse the JSON string to get the array of filenames
        const files = JSON.parse(options.parquetFiles.replace(/'/g, '"'));
        // Pass as JSON string in URL parameter
        params.set('parquetFiles', JSON.stringify(files));
      } catch (e) {
        console.warn('Failed to parse parquetFiles, using as single file:', options.parquetFiles);
        params.set('parquetFiles', JSON.stringify([options.parquetFiles]));
      }
    }
    
    if (options.defaultZoom !== undefined) {
      params.set('defaultZoom', options.defaultZoom.toString());
    }
    
    // Combine base URL with parameters if any config exists
    const finalSrc = params.toString() ? `${src}?${params.toString()}` : src;
    
    // Create the iframe node with standard attributes
    const iframeNode = {
      type: 'iframe',
      src: finalSrc,
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
    
    // Generate a unique ID for this iframe (for potential future use)
    iframeNode.id = `polar-map-${Math.random().toString(36).substr(2, 9)}`;
    
    // Return just the iframe - no script injection needed
    return [iframeNode];
  }
};

const plugin = {
  name: 'Polar Map iframe plugin',
  directives: [polarMapDirective]
};

export default plugin;
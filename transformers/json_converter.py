from typing import Any, Dict, List, Optional
from json_converter import JsonConverter
from .base import BaseTransformer

class JsonConverterTransformation(BaseTransformer):
    """Transformer that uses json-converter to transform data according to a template."""
    
    def __init__(self, params: Dict[str, Any]):
        """Initialize the JSON converter transformer.
        
        Args:
            params: Configuration parameters including:
                - template: The template to use for conversion
                - options: Optional configuration for the converter
        """
        super().__init__(params)
        self.template = params.get('template', {})
        self.options = params.get('options', {})
        self.converter = JsonConverter(**self.options)
        
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform the data using the json-converter template.
        
        Args:
            data: List of dictionaries containing the data to transform
            
        Returns:
            List of dictionaries containing the transformed data
        """
        if not data:
            return []
            
        transformed_data = []
        for entry in data:
            try:
                # Convert the entry according to the template
                transformed = self.converter.convert(entry, self.template)
                transformed_data.append(transformed)
            except Exception as e:
                self.logger.error(f"Error transforming entry: {str(e)}")
                raise
                
        return transformed_data 
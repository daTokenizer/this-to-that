from typing import Dict, Any
from controller import Transformation, MISSING_DATA_DEFAULT_VALUE, FIXED_CUSTOM_VALUE_KEY
import logging

logger = logging.getLogger("ETLController")

class MapTransformation(Transformation):
    """Transformation that maps fields from source to target according to mapping rules."""
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the transformation with the provided mapping configuration."""
        self.mapping = config.get('mapping', {})
        if not self.mapping:
            logger.warning("No mapping configuration provided, will return empty dictionary")
    
    def transform(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Transform the entry according to the mapping rules."""
        target_entry = {}
        
        for target_field, source_field in self.mapping.items():
            # Support for direct field mapping
            if isinstance(source_field, str):
                target_entry[target_field] = entry.get(source_field, MISSING_DATA_DEFAULT_VALUE)
                if source_field not in entry:
                    logger.warning(f"Source field '{source_field}' not found in entry, replacing with default value: {MISSING_DATA_DEFAULT_VALUE}")
            
            # Support for list mapping
            elif isinstance(source_field, list):
                target_entry[target_field] = [
                    entry.get(key, MISSING_DATA_DEFAULT_VALUE) for key in source_field
                ]
            
            # Support for dictionary mapping
            elif isinstance(source_field, dict):
                if FIXED_CUSTOM_VALUE_KEY in source_field:  # Support for constant values
                    target_entry[target_field] = source_field[FIXED_CUSTOM_VALUE_KEY]
                else:
                    target_entry[target_field] = {
                        k: entry.get(v, MISSING_DATA_DEFAULT_VALUE) for k, v in source_field.items()
                    }
        
        return target_entry

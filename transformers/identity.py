from typing import Dict, Any
from controller import Transformation

class IdentityTransformation(Transformation):
    """Transformation that returns the input entry unchanged."""
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the transformation with the provided parameters."""
        # Identity transformation doesn't need any configuration
        pass
    
    def transform(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Return the input entry unchanged."""
        return entry.copy()

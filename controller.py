import importlib
import logging
import json
import yaml
import os
from typing import Dict, Any, List, Iterable, Optional, Tuple
from abc import ABC, abstractmethod

CONF_FILE_PATH = "config.yml"
MISSING_DATA_DEFAULT_VALUE = None
FIXED_CUSTOM_VALUE_KEY = "FIXED_CUSTOM_VALUE"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_controller.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ETLController")

class DataSource(ABC):
    """Abstract base class for all data sources."""
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the data source with the provided parameters."""
        pass
    
    @abstractmethod
    def get_entries(self) -> List[Dict[str, Any]]:
        """Retrieve all entries from the data source."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close any open connections to the data source."""
        pass

class DataTarget(ABC):
    """Abstract base class for all data targets."""
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the data target with the provided parameters."""
        pass
    
    @abstractmethod
    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        """Create a new entry in the data target."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close any open connections to the data target."""
        pass

class Transformation(ABC):
    """Abstract base class for all data transformations."""
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the transformation with the provided parameters."""
        pass
    
    @abstractmethod
    def transform(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single entry according to the transformation rules."""
        pass

def load_config(config_path: str) -> Dict[str, Any]:
    """Load the configuration from file."""
    if not isinstance(config_path, str):
        raise ValueError("config_path must be a string")
    
    try:
        if config_path.endswith('.json'):
            with open(config_path, 'r') as f:
                return json.load(f)
        elif config_path.endswith(('.yaml', '.yml')):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            raise ValueError(f"Unsupported config file format: {config_path}")
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        raise

def validate_config(config: Dict[str, Any]) -> bool:
    if not config:
        return False

    
    # TODO: additional validations , including mandatory fields, etc.
    source = config.get('source')
    if not source:
        logger.error("must provide source data under key: \"source\"")
        return False
    source_name = source.get('name')
    if not source_name:
        logger.error("must provide source name under key: \"name\" in the \"source\" section")
        return False
    
    source = config.get('target')
    if not source:
        logger.error("must provide target data under key: \"target\"")
        return False
    source_name = source.get('name')
    if not source_name:
        logger.error("must provide target name under key: \"name\" in the \"target\" section")
        return False
    
    return True

def map_data_for_target(source_asset_data: dict, mapping: dict|None):
    if not mapping:
        return source_asset_data

    mapped_data = {}

    for target_key, source_key in mapping.items():
        if isinstance(source_key, str):
            mapped_data[target_key] = source_asset_data.get(source_key, MISSING_DATA_DEFAULT_VALUE)
        elif isinstance(source_key, list):
            mapped_data[target_key] = [
                source_asset_data.get(key, MISSING_DATA_DEFAULT_VALUE) for key in source_key
            ]
        elif isinstance(source_key, dict):
            mapped_data[target_key] = {
                v:source_asset_data.get(k, MISSING_DATA_DEFAULT_VALUE) for k,v in source_key.items()
            }

    return mapped_data

def load_source(source_name: str, params: Dict[str, Any]) -> DataSource:
    """Load and initialize the data source specified in the config."""
    
    if not source_name:
        logger.error("Source name not specified in config")
        raise ValueError("Source name not specified in config")
    
    try:
        logger.info(f"Loading source module: {source_name}")
        module_path = f"sources.{source_name}"
        source_module = importlib.import_module(module_path)
        source_class = getattr(source_module, f"{source_name.capitalize()}Source")
        
        source = source_class()
        logger.info(f"Initializing source: {source_name}")
        source.initialize(params)
        return source
    except Exception as e:
        logger.error(f"Failed to load source {source_name}: {e}")
        raise ImportError(f"Failed to load source {source_name}: {e}")

def load_target(target_name: str, params: Dict[str, Any]) -> DataTarget:
    """Load and initialize the data target specified in the config."""
    if not target_name:
        logger.error("Target name not specified in config")
        raise ValueError("Target name not specified in config")
    
    try:
        logger.info(f"Loading target module: {target_name}")
        module_path = f"targets.{target_name}"
        target_module = importlib.import_module(module_path)
        print("XXXX", target_module)
        target_class = getattr(target_module, f"{target_name.capitalize()}Target")
        
        target = target_class()
        logger.info(f"Initializing target: {target_name}")
        target.initialize(params)
        return target
    except Exception as e:
        logger.error(f"Failed to load target {target_name}: {e}")
        raise ImportError(f"Failed to load target {target_name}: {e}")

def load_transformation(transformation_name: str, params: Dict[str, Any]) -> Transformation:
    """Load and initialize the transformation specified in the config."""
    if not transformation_name:
        logger.warning("No transformation specified, using identity transformation")
        transformation_name = "identity"
        params = {}
        
    try:
        logger.info(f"Loading transformation module: {transformation_name}")
        module_path = f"transformations.{transformation_name}"
        transform_module = importlib.import_module(module_path)
        transform_class = getattr(transform_module, f"{transformation_name.capitalize()}Transformation")
        
        transformation = transform_class()
        logger.info(f"Initializing transformation: {transformation_name}")
        transformation.initialize(params)
        return transformation
    except Exception as e:
        logger.error(f"Failed to load transformation {transformation_name}: {e}")
        raise ImportError(f"Failed to load transformation {transformation_name}: {e}")


class ETLController:
    """Controller for ETL operations."""
    def __init__(self, config_path: str):
        """Initialize the ETL controller with a configuration file path."""
        logger.info(f"Initializing ETL controller with config: {config_path}")
        self.config_path = config_path
        self.config = load_config(config_path)
        if not self.config or not validate_config(self.config):
            raise ValueError("Empty or Invalid Configuration Found, halting.")
        self.source = None
        self.target = None
        self.transformation = None

    def _load_all_modules(self, force_reload: bool = False) -> Tuple[DataSource, DataTarget, Transformation]:
        # Load and initialize source, target, and transformation
        try:
            if self.source is None or force_reload:
                self._load_source()
            if self.target is None or force_reload:
                self._load_target()
            if self.transformation is None or force_reload:
                self._load_transformation()
            
            return self.source, self.target, self.transformation
        except Exception as e:
            logger.error(f"Failed to load all modules: {e}")
            raise

    def _load_source(self) -> DataSource:
        source_name = self.config.get('source', {}).get('name')
        source_params = self.config.get('source', {}).get('params', {})
        self.source = load_source(source_name, source_params)
        return self.source
    
    def _load_target(self) -> DataTarget:
        """Load and initialize the data target specified in the config."""
        target_name = self.config.get('target', {}).get('name')
        target_params = self.config.get('target', {}).get('params', {})
        self.target = load_target(target_name, target_params)
        return self.target
    
    def _load_transformation(self) -> Transformation:
        """Load and initialize the transformation specified in the config."""
        transformation_name = self.config.get('transformation', {}).get('name')
        transformation_params = self.config.get('transformation', {}).get('params', {})
        self.transformation = load_transformation(transformation_name, transformation_params)
        return self.transformation
    
    def run(self) -> int:
        """Execute the ETL process based on the configuration."""
        processed_count = 0
        error_count = 0
        
        try:
            logger.info("Starting ETL process")
            
            self._load_all_modules()
            
            # Get entries from source
            logger.info("Retrieving entries from source")
            try:
                source_entries = self.source.get_entries()
                logger.info(f"Retrieved {len(source_entries)} entries from source")
            except Exception as e:
                logger.error(f"Error retrieving entries from source: {e}")
                return 0
            
            # Process each entry
            for i, source_entry in enumerate(source_entries):
                try:
                    # Transform entry
                    transformed_entry = self.transformation.transform(source_entry)
                    
                    # Create entry in target
                    self.target.create_entries([transformed_entry])
                    processed_count += 1
                    
                    # Log progress for every 100 entries
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count} entries")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing entry {i}: {e}")
            
            logger.info(f"ETL process completed. Processed: {processed_count}, Errors: {error_count}")
            return processed_count
        
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        
        finally:
            # Clean up resources
            if self.source:
                logger.info("Closing source connection")
                self.source.close()
            
            if self.target:
                logger.info("Closing target connection")
                self.target.close()


# Example usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python etl_controller.py <config_file>")
        sys.exit(1)
    
    config_path = sys.argv[1] if len(sys.argv) == 2 else CONF_FILE_PATH
    controller = ETLController(config_path)
    
    try:
        processed_count = controller.run()
        print(f"Successfully processed {processed_count} entries")
        sys.exit(0)
    except Exception as e:
        print(f"ETL process failed: {e}")
        sys.exit(1)
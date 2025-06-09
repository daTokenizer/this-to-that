import importlib
import logging
import argparse
import json
import sys
import time
from symtable import Class

import yaml
from typing import Dict, Any, List, Iterable, Optional, Tuple
from abc import ABC, abstractmethod

CONF_FILE_PATH = "config.yml"
LOG_FILE_PATH = "etl_controller.log"
MISSING_DATA_DEFAULT_VALUE = None
FIXED_CUSTOM_VALUE_KEY = "FIXED_CUSTOM_VALUE"
DEFAULT_TRANSFORMATION_NAME = "identity"
DEFAULT_POLLING_FREQUENCY_SECONDS = 15 * 60  # Default polling frequency in seconds (15 minutes)

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

def validate_config_structure(config: Dict[str, Any]) -> bool:
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
    
    target = config.get('target')
    if not target:
        logger.error("must provide target data under key: \"target\"")
        return False
    target_name = target.get('name')
    if not target_name:
        logger.error("must provide target name under key: \"name\" in the \"target\" section")
        return False
    

    if "transformation" in config:
        transformation = config.get('transformation')
        if not isinstance(transformation, dict):
            logger.error("transformation must be a dictionary")
            return False
        transformation_name = transformation.get('name')
        if not transformation_name:
            logger.error("must provide transformation name under key: \"name\" in the \"transformation\" section")
            return False
    
    return True

def validate_modules_exist(config: Dict[str, Any]) -> bool:
    """Check if the specified source, target, and transformation modules exist."""
    try:
        source_name = config.get('source', {}).get('name')
        if not try_get_source_class(source_name):
            logger.error(f"Could not find source {source_name}")
            return False

        target_name = config.get('target', {}).get('name')
        if not try_get_target_class(target_name):
            logger.error(f"Could not find target {target_name}")
            return False

        transformation_name = config.get('transformation', {}).get('name', 'identity')
        if not try_get_transformation_class(transformation_name):
            logger.error(f"Could not find transformation {transformation_name}")
            return False
    except Exception as e:
        logger.error(f"failed to validate existence of requested modules: {e}")
        return False

    return True

def validate_config(config: Dict[str, Any]) -> bool:
    if not config or not isinstance(config, dict):
        return False

    if not validate_config_structure(config):
        logger.error("Invalid configuration structure")
        return False

    if not validate_modules_exist(config):
        logger.error("Required modules do not exist")
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

def try_get_module_class_from_path(module_name: str, module_path: str, class_name: str) -> Optional[Class]:
    try:
        logger.info(f"Loading module: {module_name} from {module_path}")
        module = importlib.import_module(module_path)

        if not module:
            logger.error(f"Failed to load module: {module_name}")
            return None

        if not hasattr(module, class_name):
            logger.info(f"Class {class_name} not found in module {module_path}")
            return None

        return getattr(module, class_name)
    except Exception as e:
        logger.error(f"Failed to get class {class_name} from {module_path}: {e}")
        return None



def try_get_module_class(module_type:str, module_name: str) -> Optional[Class]:
    """Try to get the source class from the specified module."""
    try:
        logger.debug(f"trying to get class for {module_type} called {module_name}")
        module_class_name = f"{module_name.capitalize()}{module_type.capitalize()}"
        module_module_path = f"{module_type.lower()}s"
        module_paths = [
            f"{module_module_path}.{module_name.lower()}",
            f"{module_module_path}.{module_name.lower()}_{module_type}"
        ]
        for path in module_paths:
            module_class = try_get_module_class_from_path(module_name, path, module_class_name)
            if module_class:
                return module_class

        return None
    except Exception as e:
        logger.error(f"Failed to get target class: {e}")
        return None

def try_get_source_class(source_name: str) -> Optional[Class]:
    """Try to get the source class from the specified module."""
    return try_get_module_class(module_type="source", module_name=source_name)

def try_get_target_class(target_name: str) -> Optional[Class]:
    """Try to get the source class from the specified module."""
    return try_get_module_class(module_type="target", module_name=target_name)

def try_get_transformation_class(transformation_name: str) -> Optional[Class]:
    """Try to get the source class from the specified module."""
    if not transformation_name:
        logger.warning(f"No transformation specified, using default transformation: {DEFAULT_TRANSFORMATION_NAME}")
        transformation_name = DEFAULT_TRANSFORMATION_NAME

    transformations_module_dir = "transformers"
    transformations_module_path = f"{transformations_module_dir}.{transformation_name}"
    transformation_class_name = f"{transformation_name.capitalize()}Transformation"
    transformation_class = try_get_module_class_from_path(
        transformation_name,
        module_path=transformations_module_path,
        class_name=transformation_class_name
    )

    return transformation_class

def try_instantiate_module(module_type:str, module_name:str, module_class: Class, params: Dict[str, Any]) -> Optional[DataSource|Transformation|DataTarget]:
    if not module_class:
        logger.error(f"Failed to find {module_type} {module_name}")
        raise ImportError(f"Failed to find {module_type} {module_name}")

    module_instance = module_class()

    if not module_instance:
        logger.error(f"Failed to load {module_type} {module_name}")
        raise ImportError(f"Failed to load {module_type} {module_name}")
    else:
        if hasattr(module_instance, "initialize"):
            logger.info(f"Initializing {module_type}: {module_name} with params: {params}")
            module_instance.initialize(params)
        else:
            logger.debug(f"No initialize method found for {module_type} {module_name}")

    return module_instance


def load_source(source_name: str, params: Dict[str, Any]) -> DataSource:
    """Load and initialize the data source specified in the config."""

    if not source_name:
        logger.error("Source name not specified in config")
        raise ValueError("Source name not specified in config")

    try:
        source_class = try_get_source_class(source_name)
        source = try_instantiate_module(
            module_type="source",
            module_name=source_name,
            module_class=source_class,
            params=params)

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
        target_class = try_get_target_class(target_name)
        target = try_instantiate_module(
            module_type="target",
            module_name=target_name,
            module_class=target_class,
            params=params)

        return target
    except Exception as e:
        logger.error(f"Failed to load target {target_name}: {e}")
        raise ImportError(f"Failed to load target {target_name}: {e}")

def load_transformation(transformation_name: str, params: Dict[str, Any]) -> Transformation:
    """Load and initialize the transformation specified in the config."""
    try:
        transformation_class = try_get_transformation_class(transformation_name)
        transformation = try_instantiate_module(
            module_type="transformation",
            module_name=transformation_name,
            module_class=transformation_class,
            params=params)
        return transformation
    except Exception as e:
        logger.error(f"Failed to load transformation {transformation_name}: {e}")
        raise ImportError(f"Failed to load transformation {transformation_name}: {e}")

def load_source_from_config(config) -> DataSource:
    source_name = config.get('source', {}).get('name')
    source_params = config.get('source', {}).get('params', {})
    return load_source(source_name, source_params)

def load_transformation_from_config(config) -> Transformation:
    """Load and initialize the transformation specified in the config."""
    transformation_name = config.get('transformation', {}).get('name')
    transformation_params = config.get('transformation', {}).get('params', {})
    return load_transformation(transformation_name, transformation_params)

def load_target_from_config(config) -> DataTarget:
    """Load and initialize the data target specified in the config."""
    target_name = config.get('target', {}).get('name')
    target_params = config.get('target', {}).get('params', {})

    return load_target(target_name, target_params)


class ETLController:
    """Controller for ETL operations."""
    def __init__(self, config_path: str,
                 polling_frequency: int = None,
                 source: DataSource = None,
                 transformation: Transformation = None,
                 target: DataTarget = None):
        """Initialize the ETL controller with a configuration file path."""
        logger.info(f"Initializing ETL controller with config: {config_path}")
        self.source = source
        self.transformation = transformation
        self.target = target
        self.polling_frequency = polling_frequency

        if any([source, transformation, target]) and self.polling_frequency is None:
            self.polling_frequency = DEFAULT_POLLING_FREQUENCY_SECONDS

        self.config = None
        if not any([self.source, self.transformation, self.target]):
            logger.info("No source, transformation, or target provided, loading from config")
            if not config_path:
                raise ValueError("No configuration file path provided and no modules specified")

            self.config = load_config(config_path)
            if not self.config or not validate_config(self.config):
                raise ValueError("Empty or Invalid Configuration Found, halting.")
            logger.debug(f"Loaded configuration: {self.config}")

            if self.polling_frequency is None:
                self.polling_frequency = self.config.get('polling_frequency_seconds', DEFAULT_POLLING_FREQUENCY_SECONDS)


        if self.polling_frequency is None:
            self.polling_frequency = DEFAULT_POLLING_FREQUENCY_SECONDS


    def _load_all_modules(self, force_reload: bool = False) -> Tuple[DataSource, DataTarget, Transformation]:
        # Load and initialize source, target, and transformation
        try:
            if self.source is None or force_reload:
                self.source = load_source_from_config(config=self.config)
            if self.transformation is None or force_reload:
                self.transformation = load_transformation_from_config(self.config)
            if self.target is None or force_reload:
                self.target = load_target_from_config(config=self.config)

            logger.debug(f"Source: {self.source}, Target: {self.target}, Transformation: {self.transformation}")
            return self.source, self.target, self.transformation
        except Exception as e:
            logger.error(f"Failed to load all modules: {e}")
            raise

    def run(self):
        """Run the ETL process."""
        logger.info("Running ETL process")
        try:
            while True:
                # Load source, target, and transformation modules
                if self.config:
                    logger.info("Re-loading all modules from configuration")
                    self._load_all_modules()

                logger.error(f"XXXX Source: {self.source}, Target: {self.target}, Transformation: {self.transformation}")

                # Pull data from source and push to target
                processed_count = self.process_cycle()
                logger.info(f"ETL process completed successfully. Processed {processed_count} entries.")
                # Wait for the next polling interval
                if self.polling_frequency is None or self.polling_frequency <= 0:
                    logger.warning("Polling frequency is set to 0 or negative, exiting ETL process")
                    break
                else:
                    logger.info(f"Waiting for {self.polling_frequency} seconds ({self.polling_frequency/60}) min before next run")
                    time.sleep(self.polling_frequency)

        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        finally:
            # Clean up resources
            if self.source:
                self.source.close()
            if self.target:
                self.target.close()


    def process_cycle(self) -> int:
        """Execute the ETL process based on the configuration."""
        processed_count = 0
        error_count = 0

        try:
            logger.info("Starting cycle of ETL process")

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

def main():
    """Main entry point for the ETL controller."""
    parser = argparse.ArgumentParser(description='ETL Controller')
    parser.add_argument('--config', '-c',
                       default=CONF_FILE_PATH,
                       help='Path to configuration file')
    parser.add_argument('--log-file', '-l',
                       default=LOG_FILE_PATH,
                       help='Path to log file')

    args = parser.parse_args()

    # Configure logging if log file specified
    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    controller = ETLController(args.config)

    try:
        processed_count = controller.run()
        print(f"Successfully processed {processed_count} entries")
        sys.exit(0)
    except Exception as e:
        print(f"ETL process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# PyETL

A high-performance, functional Python ETL (Extract, Transform, Load) framework designed for efficient data processing and transfer between systems. Currently supports data extraction from Crowdstrike and loading into Sepio.

## Features

- **Modular Architecture**: Clean separation of concerns with source, target, and transform components
- **High Performance**: Optimized for large-scale data processing with parallel execution
- **Functional Design**: Written with functional programming principles for better reliability and testability
- **Robust Error Handling**: Comprehensive error handling and retry mechanisms
- **Configurable**: YAML/JSON based configuration system
- **Extensible**: Easy to add new data sources and targets

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/pyetl.git
cd pyetl

# Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

The ETL process is configured using a YAML or JSON file. Here's an example configuration:

```yaml
source:
  name: crowdstrike
  params:
    auth_url: "https://auth.example.com"
    client_id: "your_client_id"
    client_secret: "your_client_secret"
    get_asset_ids_url: "https://assets.example.com/ids"
    get_asset_data_url: "https://assets.example.com/data"

target:
  name: sepio
  params:
    sepio_url: "https://sepio.example.com/api"
    batch_size: 100
    source_name: "crowdstrike"
    max_retries: 3

mapping:
  target_id: "source_id"
  target_name: "hostname"
  target_mac: "mac_address"
  target_type:
    value: "device"
```

## Usage

### Basic Usage

```bash
python controller.py config.yml
```

### Environment Variables

You can override authentication credentials using environment variables:

```bash
export AUTH_URL="https://auth.example.com"
export CLIENT_ID="your_client_id"
export CLIENT_SECRET="your_client_secret"
```

## Architecture

### Components

1. **Controller**: Orchestrates the ETL process
2. **Sources**: Data extraction modules (e.g., Crowdstrike)
3. **Targets**: Data loading modules (e.g., Sepio)
4. **Transforms**: Data transformation modules

### Data Flow

1. Source extracts data using parallel processing
2. Data is transformed according to mapping rules
3. Target loads data in configurable batch sizes
4. Error handling and retries at each stage

## Development

### Adding a New Source

1. Create a new file in `sources/`
2. Implement the `DataSource` interface
3. Add corresponding tests in `tests/`

### Adding a New Target

1. Create a new file in `targets/`
2. Implement the `DataTarget` interface
3. Add corresponding tests in `tests/`

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_controller.py

# Run with verbose output
pytest -v
```

## Performance Considerations

- Batch processing for efficient data transfer
- Parallel processing for data extraction
- Configurable batch sizes and retry mechanisms
- Memory-efficient generators for large datasets

## Error Handling

- Automatic retries for transient failures
- Comprehensive logging
- Graceful error recovery
- Detailed error reporting

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license]

## Support

For support, please [open an issue](https://github.com/yourusername/pyetl/issues) or contact [your contact information]. 
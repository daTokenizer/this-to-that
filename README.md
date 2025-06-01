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

# Containerized ETL System

This ETL (Extract, Transform, Load) system is containerized using Docker for easy deployment and management. The system includes automatic log rotation and management to prevent disk space issues.

## Prerequisites

- Docker installed on your system
- A valid ETL configuration file (YAML format)
- Sufficient disk space for logs (recommended: at least 1GB)

## Quick Start

1. Build the Docker image:
```bash
docker build -t etl-container .
```

2. Run the ETL process:
```bash
docker run -v /path/to/logs:/var/log/etl \
          -v /path/to/config.yml:/app/config.yml \
          etl-container config.yml
```

## Configuration

### Volume Mounts

The container requires two volume mounts:

1. Log Directory:
   - Host path: `/path/to/logs`
   - Container path: `/var/log/etl`
   - Purpose: Stores all ETL logs with automatic rotation

2. Config File:
   - Host path: `/path/to/config.yml`
   - Container path: `/app/config.yml`
   - Purpose: Provides ETL configuration

### Log Management

- Logs are stored in the mounted volume at `/path/to/logs`
- Daily log rotation is enabled
- Logs are kept for 7 days
- Old logs are automatically compressed
- Log files are named with the format: `etl_YYYYMMDD.log`

## Example Usage

### Basic Run
```bash
docker run -v $(pwd)/logs:/var/log/etl \
          -v $(pwd)/config.yml:/app/config.yml \
          etl-container config.yml
```

### Run with Custom Log Directory
```bash
docker run -v /var/log/my-etl:/var/log/etl \
          -v $(pwd)/config.yml:/app/config.yml \
          etl-container config.yml
```

## Log Rotation Configuration

The system uses logrotate with the following settings:
- Daily rotation
- 7 days of log retention
- Automatic compression of old logs
- Log files are created with 0640 permissions

## Troubleshooting

1. **Container fails to start**
   - Ensure the config file exists and is properly mounted
   - Check file permissions on the log directory
   - Verify the config file is valid YAML

2. **No logs appearing**
   - Check if the log directory is properly mounted
   - Verify write permissions on the log directory
   - Check container logs using `docker logs <container-id>`

3. **Disk space issues**
   - Logs are automatically rotated and compressed
   - Old logs are automatically removed after 7 days
   - Monitor disk usage in the log directory

## Security Considerations

- Log files are created with restricted permissions (0640)
- The container runs with minimal privileges
- Sensitive data in logs should be properly handled in the ETL configuration

## Maintenance

To clean up old containers and images:

```bash
# Remove stopped containers
docker container prune

# Remove unused images
docker image prune
```

## Support

For issues or questions, please check the project documentation or create an issue in the repository. 
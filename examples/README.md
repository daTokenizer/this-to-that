# Endpoint Source/Target Example

This example demonstrates how to use the endpoint source and target components to transfer data between two REST APIs.

## Configuration Overview

The `endpoint_config.yml` file shows how to configure the ETL process to:
1. Fetch user data from a source API
2. Optionally transform the data
3. Send the data to a target API in batches

## Configuration Details

### Source Configuration
```yaml
source:
  name: endpoint
  params:
    base_url: "https://api.example.com"  # Base URL of the source API
    endpoint: "/users"                   # API endpoint to fetch data from
    headers:                             # HTTP headers for the request
      Authorization: "Bearer source_api_token"
      Accept: "application/json"
    params:                              # Query parameters
      limit: 100
      status: "active"
    auth:                                # Basic authentication
      - "source_username"
      - "source_password"
    verify_ssl: true                     # SSL certificate verification
    timeout: 30                          # Request timeout in seconds
```

### Target Configuration
```yaml
target:
  name: endpoint
  params:
    base_url: "https://api.target.com"   # Base URL of the target API
    endpoint: "/users/sync"              # API endpoint to send data to
    headers:                             # HTTP headers for the request
      Authorization: "Bearer target_api_token"
      Content-Type: "application/json"
    auth:                                # Basic authentication
      - "target_username"
      - "target_password"
    verify_ssl: true                     # SSL certificate verification
    timeout: 30                          # Request timeout in seconds
    method: "POST"                       # HTTP method to use
    batch_size: 50                       # Number of entries per request
```

## SQL Modules

The ETL framework includes SQL source and target modules for database operations.

### SQL Source Configuration
```yaml
source:
  name: sql
  params:
    # Connection details
    host: "localhost"
    port: 5432
    database: "source_db"
    username: "db_user"
    password: "db_password"
    # Or use connection URL
    # url: "postgresql://user:pass@localhost:5432/dbname"
    
    # Query configuration
    query: "SELECT * FROM users WHERE status = :status"
    query_params:
      status: "active"
    
    # Optional schema support
    schema: "public"
```

### SQL Target Configuration
```yaml
target:
  name: sql
  params:
    # Connection details
    host: "localhost"
    port: 5432
    database: "target_db"
    username: "db_user"
    password: "db_password"
    # Or use connection URL
    # url: "postgresql://user:pass@localhost:5432/dbname"
    
    # Table configuration
    table: "users"
    schema: "public"  # Optional
    
    # Column definitions
    columns:
      id:
        type: "integer"
        primary_key: true
      name:
        type: "varchar"
        length: 255
      email:
        type: "varchar"
        length: 255
      created_at:
        type: "timestamp"
```

## Transformations

The ETL framework supports various transformations to modify data during the transfer process.

### Identity Transformation
```yaml
transformation:
  name: identity
  params: {}  # No parameters needed
```
Passes data through without modification.

### Field Mapping Transformation
```yaml
transformation:
  name: field_mapping
  params:
    mappings:
      target_field: "source_field"  # Direct mapping
      full_name: ["first_name", "last_name"]  # List of source fields
      address:  # Nested mapping
        street: "street_address"
        city: "city_name"
```

### Filter Transformation
```yaml
transformation:
  name: filter
  params:
    conditions:
      - field: "status"
        operator: "eq"
        value: "active"
      - field: "age"
        operator: "gt"
        value: 18
```

### Aggregate Transformation
```yaml
transformation:
  name: aggregate
  params:
    group_by: ["category", "status"]
    aggregations:
      total_count:
        function: "count"
        field: "id"
      average_value:
        function: "avg"
        field: "value"
```

### JSON Converter Transformation
```yaml
transformation:
  name: json_converter
  params:
    template:
      user_id: "user.id"
      full_name: "user.name"
      contact:
        email: "user.email"
        location:
          city: "user.address.city"
          state: "user.address.state"
    options:
      default_value: null
      strict: true
```
Uses json-converter package to transform data according to a template. The template defines the structure of the output and uses dot notation to reference fields from the input data.

## Usage

1. Update the configuration with your actual API details:
   - Replace the example URLs with your actual API endpoints
   - Update authentication credentials
   - Adjust headers and parameters as needed
   - Modify batch size based on your API limits

2. Run the ETL process:
   ```bash
   python controller.py --config examples/endpoint_config.yml
   ```

## Features

- **Source Features**:
  - Fetches data from REST APIs
  - Supports query parameters
  - Handles authentication
  - Processes JSON responses
  - Configurable timeout and SSL verification

- **Target Features**:
  - Sends data to REST APIs
  - Supports multiple HTTP methods (POST, PUT, PATCH)
  - Batch processing for large datasets
  - Configurable batch size
  - Handles authentication
  - Configurable timeout and SSL verification

- **SQL Features**:
  - Support for multiple database types (PostgreSQL, MySQL, SQLite)
  - Connection pooling
  - Parameterized queries
  - Schema support
  - Automatic table creation
  - Batch processing
  - Transaction support

- **Transformation Features**:
  - Field mapping and renaming
  - Data filtering
  - Aggregation
  - Custom transformations
  - Chaining multiple transformations
  - JSON structure transformation with templates
  - Deep nested field mapping
  - Default value handling

## Error Handling

The configuration includes error handling for:
- Connection timeouts
- Authentication failures
- Invalid responses
- SSL certificate issues
- Database connection errors
- Query execution errors
- Data validation errors

## Security Notes

1. Never commit real API tokens or credentials to version control
2. Consider using environment variables for sensitive data
3. Use HTTPS endpoints with proper SSL verification
4. Implement proper access controls on both source and target APIs
5. Use parameterized queries to prevent SQL injection
6. Implement proper database user permissions
7. Encrypt sensitive data in transit and at rest 
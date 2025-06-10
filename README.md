# ETL Pipeline Manager

A modern web application for managing ETL (Extract, Transform, Load) pipelines using Python FastAPI and Material Design.

## Features

- Real-time pipeline monitoring with WebSocket support
- Docker-based pipeline execution
- Modern Material Design UI
- RESTful API for pipeline management
- Pipeline configuration management
- Real-time logs and status updates

## Project Structure

```
.
├── web/                    # Web application
│   ├── api/               # API endpoints
│   ├── core/              # Core configuration
│   ├── db/                # Database models and session
│   ├── schemas/           # Pydantic schemas
│   ├── services/          # Business logic services
│   └── main.py            # FastAPI application
├── frontend/              # React frontend
│   ├── src/              # Source code
│   │   ├── components/   # React components
│   │   ├── services/     # API and WebSocket services
│   │   └── App.tsx       # Main application component
│   ├── public/           # Static files
│   └── package.json      # Frontend dependencies
├── docker/               # Docker configuration
│   ├── backend/         # Backend Dockerfile
│   └── frontend/        # Frontend Dockerfile
└── run.py               # Unified development server script
```

## Prerequisites

- Python 3.8+
- Node.js 14+
- Docker
- PostgreSQL

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/pyetl.git
cd pyetl
```

2. Set up the backend:
```bash
cd web
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your configuration
```

3. Set up the frontend:
```bash
cd frontend
npm install
```

## Running the Application

### Quick Start (Recommended)

The easiest way to start both the backend and frontend servers is to use the unified script:

```bash
./run.py
```

This script will:
- Check and install any missing dependencies
- Start both the backend and frontend servers
- Stream the output from both servers
- Handle graceful shutdown on Ctrl+C

Available options:
```bash
./run.py --help
```

Options:
- `--no-reload`: Disable backend auto-reload
- `--backend-port`: Specify backend server port (default: 8000)
- `--frontend-port`: Specify frontend server port (default: 3000)

### Manual Start

Alternatively, you can start the servers manually:

Backend:
```bash
cd web
uvicorn main:app --reload
```

Frontend:
```bash
cd frontend
npm start
```

## Docker Deployment

1. Build the Docker images:
```bash
docker-compose build
```

2. Start the containers:
```bash
docker-compose up -d
```

## API Documentation

Once the application is running, you can access the API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## WebSocket API

The application provides real-time updates through WebSocket connections:

```javascript
const ws = new WebSocket(`ws://localhost:8000/ws/pipelines/${pipelineId}`);

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // Handle different message types:
  // - status: Pipeline status updates
  // - logs: New log entries
  // - error: Error messages
};
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
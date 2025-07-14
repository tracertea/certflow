# Certflow

## Overview

Certflow is a Go-based tool designed for efficiently retrieving and processing certificates from Certificate Transparency logs. It utilizes concurrent operations, proxy management, and robust state handling for continuous, reliable certificate data processing.

## Features

* **Concurrent Downloading**: Speeds up certificate retrieval through parallel processing.
* **Proxy Management**: Optimizes performance and reliability by efficiently managing multiple HTTP/S proxies.
* **State Persistence**: Maintains state across sessions to prevent data loss.
* **Batch Processing & Compression**: Efficiently organizes certificates into compressed batch files.
* **Interactive Monitoring**: Provides real-time terminal-based progress monitoring and performance metrics.

## Project Structure

```
certflow
├── internal
│   ├── config         # Handles configuration files
│   ├── ctlog          # Interacts with Certificate Transparency logs
│   ├── logging        # Manages structured logging
│   ├── network        # Handles network operations and proxy management
│   ├── processing     # Formats, aggregates, and compresses data
│   ├── state          # Manages persistent state
│   └── ui             # Implements terminal-based user interface
├── main.go            # Entry point for the application
├── go.mod
└── go.sum
```

## Installation

### Prerequisites

* Go 1.24.3 or later

### Installation Steps

```bash
git clone https://github.com/tracertea/certflow.git
cd certflow
go mod download
go build -o certflow main.go
```

## Usage

```bash
./certflow -log-config path/to/loglist.json [options]
```

### Available Options

* `-log-config`: Path to log list configuration JSON file (**required**).
* `-output-dir`: Output directory (default: `./certflow_output`).
* `-continuous`: Continuous polling for new certificates (default: `true`).
* `-state-save-interval`: Interval to save progress (default: `10s`).
* `-proxies`: Comma-separated list of proxies (e.g., `http://p1:8080,http://p2:8080`).
* `-batch-size`: Number of certificates per batch (default: `100000`).

### Example

```bash
./certflow -log-config logs.json -proxies http://proxy1:8080,http://proxy2:8080
```

## TODO
- [ ] Refresh -log-config periodically
- [ ] Add support for more CT logs
- [ ] Add start index override 
- [ ] Add continue from last index
# Distributed Hash Table (DHT) Implementation

A distributed key-value store implementation using a Chord-like ring topology with support for multiple consistency models and replication strategies.

## Overview

This project implements a Distributed Hash Table (DHT) system that stores and retrieves key-value pairs across a network of nodes. The system is designed with a Chord-like ring topology where each node is responsible for a specific key range.

## Features

- **Chord-like Ring Topology**: Nodes are organized in a ring structure with consistent hashing
- **Multiple Consistency Models**:
  - Chain Replication (strong consistency)
  - Eventual Consistency (high availability)
- **Configurable Replication Factor (k)**: Control how many copies of each key-value pair exist
- **CLI Interface**: Interactive command-line interface to interact with the DHT
- **RESTful API**: HTTP endpoints for all DHT operations
- **Performance Measurement Tools**: Scripts to evaluate throughput and consistency

## Architecture

### Node Structure
- **Bootstrap Node**: Manages node joining/leaving and provides entry point to the DHT
- **Regular Node**: Stores key-value pairs and forwards requests based on the Chord protocol

### Key Operations
- **Insert**: Store a key-value pair in the DHT
- **Query**: Retrieve a value by key
- **Delete**: Remove a key-value pair from the DHT
- **Join**: Add a new node to the DHT
- **Depart**: Gracefully remove a node from the DHT

### Consistency Models

1. **Chain Replication**
   - Writes are forwarded through a chain of k replicas
   - Reads are serviced by the tail of the chain
   - Provides strong consistency but with higher latency

2. **Eventual Consistency**
   - Writes return immediately after updating the primary node
   - Replication happens asynchronously in the background
   - Reads might return stale data, but provides higher throughput

## Installation

\`\`\`bash
# Clone the repository
git clone <repository-url>
cd <repository-directory>

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
\`\`\`

## Usage

### Starting a DHT Network

1. Start the bootstrap node:
\`\`\`bash
python bootstrap.py 5000
\`\`\`

2. Start additional nodes:
\`\`\`bash
python regular_node.py 5001
python regular_node.py 5002
# ... and so on
\`\`\`

3. Have each node join the DHT:
\`\`\`bash
curl -X POST http://localhost:5001/join
curl -X POST http://localhost:5002/join
# ... and so on
\`\`\`

### Using the CLI

The CLI provides an interactive way to interact with the DHT:

\`\`\`bash
python cli.py
\`\`\`

Example commands:
\`\`\`
DHT> setnode 3
Target node set to: http://10.0.17.158:5002

DHT> insert "Like a Rolling Stone" "http://download.nodeX"
{"message": "Inserted 'Like a Rolling Stone' at node 10.0.17.158:5002"}
  
DHT> query "Like a Rolling Stone"
{"message": "Song found at node 10.0.17.158:5002", "value": "http://download.nodeX"}
  
DHT> delete "Like a Rolling Stone"
{"message": "Deleted 'Like a Rolling Stone' from node 10.0.17.158:5002"}
  
DHT> overlay
{"overlay": ["10.0.17.96:5000", "10.0.17.245:5001", "10.0.17.158:5002", "10.0.17.171:5003", "10.0.17.209:5004", "..."]}
\`\`\`

## API Reference

### Node API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| \`/insert/<key>/<value>\` | POST | Insert a key-value pair |
| \`/query/<key>\` | GET | Get value of a key |
| \`/delete/<key>\` | DELETE | Remove a key-value pair |
| \`/join\` | POST | Join the DHT network |
| \`/depart\` | POST | Gracefully leave the DHT |
| \`/overlay\` | GET | Get the current node topology |

## Running Experiments

The project includes several scripts for performance evaluation:

\`\`\`bash
# Insert throughput experiment
python insert_experiment.py

# Query throughput experiment
python query_experiment.py

# Consistency test with concurrent operations
python request_experiment.py

# Visualize results
python first_experiment.py  # For insert throughput comparison
python second_experiment.py  # For query throughput comparison
\`\`\`

## Configuration

Edit [\`utils.py\`](utils.py ) to change system parameters:

\`\`\`python
k = 2  # Replication factor
consistency = "chain replication"  # Options: "chain replication" or "eventual consistency"
\`\`\`

## Performance Results

The system demonstrates different performance characteristics depending on the consistency model and replication factor:

- **Chain Replication**: Provides strong consistency but with lower throughput as k increases
- **Eventual Consistency**: Provides higher throughput that actually improves with increased replication factor


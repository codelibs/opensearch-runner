# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenSearch Runner is a utility library that runs OpenSearch clusters on a single JVM instance for development and testing. It can be used as an embedded OpenSearch instance in applications or for running test clusters.

## Build and Development Commands

### Build
```bash
mvn compile
```

### Run Tests
```bash
mvn test
```

### Run a Single Test
```bash
mvn test -Dtest=OpenSearchRunnerTest#test_runCluster
```

### Package
```bash
mvn package
```

### Run Standalone Cluster
```bash
# Default: 3 nodes on ports 9201-9203 (HTTP) and 9301-9303 (Transport)
mvn exec:java

# Custom configuration (e.g., 4 nodes)
mvn exec:java -Dexec.args="-basePath es_home -numOfNode 4"
```

## Architecture

### Core Components

- **OpenSearchRunner** (`src/main/java/org/codelibs/opensearch/runner/OpenSearchRunner.java`): Main class that manages the lifecycle of OpenSearch nodes. Provides methods for starting/stopping nodes, performing operations (index, search, delete), and cluster management.

- **OpenSearchRunnerNode** (`src/main/java/org/codelibs/opensearch/runner/node/OpenSearchRunnerNode.java`): Custom Node implementation for running OpenSearch instances.

- **OpenSearchCurl** (`src/main/java/org/codelibs/opensearch/runner/net/OpenSearchCurl.java`): HTTP client utilities for interacting with the OpenSearch cluster.

### Key Design Patterns

1. **Builder Pattern**: The `OpenSearchRunner.Builder` interface allows customization of node settings during cluster setup.

2. **Configs Class**: Fluent API for configuring cluster parameters (number of nodes, ports, paths, plugins).

3. **Module/Plugin System**: Supports loading OpenSearch modules and plugins dynamically via `moduleTypes` and `pluginTypes` configuration.

### Default Configuration

- Base HTTP Port: 9200 (node N listens on 9200+N → 9201-9203 for 3 nodes)
- Cluster Name: "opensearch-runner"
- Data Directory: `es_home/data`
- Config Directory: `es_home/config`
- Logs Directory: `es_home/logs`

## Gotchas

- If `basePath` is not set, the runner creates a temp directory (`Files.createTempDirectory("opensearch-cluster")`) — data is not persisted across runs unless `basePath` is specified.
- Single-node clusters (`numOfNode == 1`) use special discovery settings; multi-node defaults differ.

### Testing Approach

Tests use JUnit 4 and follow the pattern:
1. Create runner instance in `setUp()`
2. Build cluster with custom configuration
3. Perform operations and assertions
4. Clean up in `tearDown()`

## Dependencies

- OpenSearch 3.7.0
- Lucene 10.4.0
- Log4j 2.25.4
- Java 21 required
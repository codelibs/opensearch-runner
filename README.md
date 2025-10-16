# OpenSearch Runner

[![Java CI with Maven](https://github.com/codelibs/opensearch-runner/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/opensearch-runner/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.codelibs.opensearch/opensearch-runner.svg)](https://repo1.maven.org/maven2/org/codelibs/opensearch/opensearch-runner/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Overview

OpenSearch Runner is a utility library that allows you to run OpenSearch clusters on a single JVM instance for development and testing purposes. It provides an easy way to programmatically start, manage, and interact with OpenSearch nodes without complex setup.

### Key Features

- **Single JVM Execution**: Run multiple OpenSearch nodes in a single JVM process
- **Embedded Usage**: Use as an embedded OpenSearch instance in your application
- **Testing Support**: Perfect for integration tests and development
- **Flexible Configuration**: Customize cluster settings, ports, and paths
- **Module & Plugin Support**: Load OpenSearch modules and plugins dynamically
- **Cluster Management**: Easy APIs for common operations (indexing, searching, cluster health)

## Requirements

- Java 21 or higher
- Maven 3.6 or higher
- OpenSearch 3.3.0 (current development version)

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>org.codelibs.opensearch</groupId>
    <artifactId>opensearch-runner</artifactId>
    <version>3.3.0.0</version>
</dependency>
```

For testing purposes, use `test` scope:

```xml
<dependency>
    <groupId>org.codelibs.opensearch</groupId>
    <artifactId>opensearch-runner</artifactId>
    <version>3.3.0.0</version>
    <scope>test</scope>
</dependency>
```

### Gradle

```gradle
implementation 'org.codelibs.opensearch:opensearch-runner:3.3.0.0'

// For testing
testImplementation 'org.codelibs.opensearch:opensearch-runner:3.3.0.0'
```

## Quick Start

### Basic Usage

```java
import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;
import org.codelibs.opensearch.runner.OpenSearchRunner;

// Create and start a single-node cluster
OpenSearchRunner runner = new OpenSearchRunner();
runner.build(newConfigs());

// Perform operations
runner.createIndex("my-index", null);
runner.insert("my-index", "1", "{ \"name\": \"OpenSearch\" }");

// Clean up
runner.close();
runner.clean(); // Optional: delete all data
```

### Custom Configuration

```java
// Configure a 3-node cluster with custom settings
OpenSearchRunner runner = new OpenSearchRunner();
runner.onBuild(new OpenSearchRunner.Builder() {
    @Override
    public void build(int nodeNumber, Settings.Builder settingsBuilder) {
        // Customize settings for each node
        settingsBuilder.put("index.number_of_replicas", 1);
        settingsBuilder.put("index.number_of_shards", 3);
    }
}).build(newConfigs()
    .numOfNode(3)
    .baseHttpPort(9201)
    .clusterName("my-test-cluster")
    .basePath("/tmp/opensearch"));
```

## API Reference

### Cluster Management

```java
// Start/stop operations
runner.build(configs);           // Build and start cluster
runner.startNode(nodeIndex);     // Start specific node
runner.close();                  // Stop all nodes
runner.clean();                  // Delete all data files

// Node access
Node node = runner.node();                    // Get any node
Node clusterManager = runner.clusterManagerNode(); // Get cluster manager node
Node nonClusterManager = runner.nonClusterManagerNode(); // Get data node
Client client = runner.client();              // Get OpenSearch client

// Cluster health
runner.ensureGreen();            // Wait for green status
runner.ensureYellow();           // Wait for yellow status
runner.waitForRelocation();      // Wait for shard relocation
```

### Index Operations

```java
// Index management
runner.createIndex("index-name", settings);
runner.deleteIndex("index-name");
runner.indexExists("index-name");
runner.openIndex("index-name");
runner.closeIndex("index-name");

// Mappings
runner.createMapping("index-name", mappingJson);
runner.createMapping("index-name", xContentBuilder);

// Aliases
runner.getAlias("alias-name");
runner.updateAlias("alias-name", addIndices, removeIndices);
```

### Document Operations

```java
// CRUD operations
runner.insert("index", "id", "{ \"field\": \"value\" }");
runner.delete("index", "id");
runner.count("index");

// Search
SearchResponse response = runner.search("index", queryBuilder, 
    sortBuilder, from, size);

// Using callback for advanced queries
runner.search("index", builder -> {
    builder.setQuery(QueryBuilders.matchAllQuery())
           .addSort("timestamp", SortOrder.DESC)
           .setSize(100);
});
```

### Maintenance Operations

```java
// Flush, refresh, and optimization
runner.flush();
runner.refresh();
runner.forceMerge();
runner.upgrade();

// Custom configurations
runner.flush(builder -> {
    builder.setIndices("index1", "index2")
           .setForce(true);
});
```

## Testing with JUnit

### JUnit 4 Example

```java
public class MyOpenSearchTest {
    private OpenSearchRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = new OpenSearchRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
        }).build(newConfigs());
    }

    @After
    public void tearDown() throws Exception {
        runner.close();
        runner.clean();
    }

    @Test
    public void testIndexing() throws Exception {
        String index = "test-index";
        
        runner.createIndex(index, null);
        runner.insert(index, "1", "{ \"message\": \"test\" }");
        runner.refresh();
        
        assertEquals(1, runner.count(index));
    }
}
```

### JUnit 5 Example

```java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MyOpenSearchTest {
    private OpenSearchRunner runner;

    @BeforeAll
    void setUp() {
        runner = new OpenSearchRunner();
        runner.build(newConfigs()
            .numOfNode(1)
            .baseHttpPort(9201));
    }

    @AfterAll
    void tearDown() {
        runner.close();
        runner.clean();
    }

    @Test
    void testSearch() {
        // Your test code here
    }
}
```

## Standalone Execution

### Setup

```bash
# Clone the repository
git clone https://github.com/codelibs/opensearch-runner.git
cd opensearch-runner

# Build the project
mvn compile
```

### Run Cluster

```bash
# Start with default configuration (3 nodes)
mvn exec:java

# Custom configuration
mvn exec:java -Dexec.args="-basePath /tmp/opensearch -numOfNode 5"

# Available options:
# -basePath <path>     : Base directory for OpenSearch data (default: es_home)
# -numOfNode <number>  : Number of nodes to start (default: 3)
```

Default ports:
- HTTP: 9201-9203 (for 3 nodes)
- Transport: 9301-9303 (for 3 nodes)

### Stop Cluster

Press `Ctrl+C` or kill the process to stop the cluster.

## Configuration Options

### Configs Builder

The `Configs` class provides a fluent API for configuration:

```java
newConfigs()
    .basePath("/path/to/data")           // Base directory
    .numOfNode(3)                        // Number of nodes
    .baseHttpPort(9201)                  // Starting HTTP port
    .clusterName("my-cluster")           // Cluster name
    .indexStoreType("niofs")             // Index store type
    .moduleTypes("org.opensearch.analysis.common.CommonAnalysisPlugin")
    .pluginTypes("com.example.MyPlugin")
    .disableESLogger()                   // Disable OpenSearch logging
    .printOnFailure()                    // Print errors on failure
    .useLogger();                        // Enable logging
```

### Settings Builder

Customize OpenSearch settings per node:

```java
runner.onBuild((nodeNumber, settingsBuilder) -> {
    // Node-specific settings
    settingsBuilder.put("node.name", "node-" + nodeNumber);
    
    // Security settings
    settingsBuilder.put("plugins.security.disabled", true);
    
    // Network settings
    settingsBuilder.put("network.host", "127.0.0.1");
    
    // Discovery settings
    settingsBuilder.put("discovery.type", "single-node");
});
```

## Advanced Features

### Custom Modules and Plugins

```java
// Load custom modules
runner.build(newConfigs()
    .moduleTypes(
        "org.opensearch.analysis.common.CommonAnalysisPlugin",
        "org.opensearch.index.reindex.ReindexPlugin"
    ));

// Load custom plugins
runner.build(newConfigs()
    .pluginTypes("com.example.MyCustomPlugin"));
```

### Working with Multiple Nodes

```java
// Access specific nodes
for (int i = 0; i < runner.getNodeSize(); i++) {
    Node node = runner.getNode(i);
    // Perform node-specific operations
}

// Get node by name
Node namedNode = runner.getNode("node-1");
```

### HTTP Client Integration

```java
// Use with OpenSearchCurl for HTTP operations
String response = OpenSearchCurl.get(node, "/_cluster/health");
String indexResponse = OpenSearchCurl.post(node, "/my-index/_doc/1", 
    "{ \"field\": \"value\" }");
```

## Troubleshooting

### Common Issues

1. **Port Already in Use**: Change the base HTTP port using `.baseHttpPort(9250)`
2. **Permission Denied**: Ensure write permissions for the base path directory
3. **Memory Issues**: Increase JVM heap size with `-Xmx2g`
4. **Plugin Loading**: Verify plugin classes are in the classpath

### Logging

Enable detailed logging for debugging:

```java
runner.build(newConfigs()
    .useLogger()           // Enable runner logging
    .printOnFailure());    // Print stack traces on failure
```

Configure Log4j2 by placing a `log4j2.properties` file in your classpath.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development

```bash
# Run tests
mvn test

# Run specific test
mvn test -Dtest=OpenSearchRunnerTest#test_runCluster

# Build package
mvn package

# Install to local repository
mvn install
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/codelibs/opensearch-runner/issues)
- **Documentation**: [API Documentation](https://codelibs.github.io/opensearch-runner/)
- **Examples**: See [test cases](src/test/java/org/codelibs/opensearch/runner/) for more examples

## Related Projects

- [OpenSearch](https://opensearch.org/) - The OpenSearch project
- [TestContainers](https://www.testcontainers.org/) - Alternative for integration testing


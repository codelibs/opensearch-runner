OpenSearch Runner
[![Java CI with Maven](https://github.com/codelibs/opensearch-runner/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/opensearch-runner/actions/workflows/maven.yml)
============

This project runs OpenSearch cluster on one JVM instance for your development/testing easily.
You can use OpenSearch Runner as Embedded OpenSearch in your application.

## Version

[Versions in Maven Repository](https://repo1.maven.org/maven2/org/codelibs/opensearch/opensearch-runner/)

## Run on Your Application

Put opensearch-runner if using Maven:

    <dependency>
        <groupId>org.codelibs.opensearch</groupId>
        <artifactId>opensearch-runner</artifactId>
        <version>x.x.x.0</version>
    </dependency>

### Start Runner

    import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;
    ...
    // create runner instance
    OpenSearchRunner runner = new OpenSearchRunner();
    // create ES nodes
    runner.onBuild(new OpenSearchRunner.Builder() {
        @Override
        public void build(final int number, final Builder settingsBuilder) {
            // put opensearch settings
            // settingsBuilder.put("index.number_of_replicas", 0);
        }
    }).build(newConfigs());

build(Configs) method configures/starts Clsuter Runner.

### Stop Runner

    // close runner
    runner.close();

### Clean up 

    // delete all files(config and index)
    runner.clean();

## Run on JUnit

Put opensearch-runner as test scope:

    <dependency>
        <groupId>org.codelibs.opensearch</groupId>
        <artifactId>opensearch-runner</artifactId>
        <version>x.x.x.0</version>
        <scope>test</scope>
    </dependency>

and see [OpenSearchRunnerTest](https://github.com/codelibs/opensearch-runner/blob/master/src/test/java/org/codelibs/opensearch/runner/OpenSearchRunnerTest.java "OpenSearchRunnerTest").

## Run as Standalone

### Install Maven

Download and install Maven 3 from http://maven.apache.org/.

### Clone This Project

    git clone https://github.com/codelibs/opensearch-runner.git

### Build This Project

    mvn compile

## Run/Stop OpenSearch Cluster

### Run Cluster

Run:

    mvn exec:java 

The default cluster has 3 nodes and the root directory for OpenSearch is es\_home.
Nodes use 9201-9203 port for HTTP and 9301-9303 port for Transport.
If you want to change the number of node, Run:

    mvn exec:java -Dexec.args="-basePath es_home -numOfNode 4"

### Stop Cluster

Type Ctrl-c or kill the process.

/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.opensearch.runner;

import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;
import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Test;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Edge case and additional test cases for OpenSearchRunner.
 */
public class OpenSearchRunnerEdgeCasesTest {

    private OpenSearchRunner runner;

    @After
    public void tearDown() throws Exception {
        if (runner != null) {
            runner.close();
            runner.clean();
        }
    }

    @Test
    public void testSingleNodeCluster() throws Exception {
        final String clusterName = "single-node-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();

        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
        }).build(newConfigs().clusterName(clusterName).numOfNode(1));

        runner.ensureYellow();

        assertEquals(1, runner.getNodeSize());
        assertNotNull(runner.node());
        assertNotNull(runner.client());
        assertNotNull(runner.clusterService());
    }

    @Test
    public void testTwoNodeCluster() throws Exception {
        final String clusterName = "two-node-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();

        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
            settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
            settingsBuilder.putList("cluster.initial_cluster_manager_nodes", "127.0.0.1:9301");
        }).build(newConfigs().clusterName(clusterName).numOfNode(2));

        runner.ensureYellow();

        assertEquals(2, runner.getNodeSize());
        assertNotNull(runner.getNode(0));
        assertNotNull(runner.getNode(1));
    }

    @Test
    public void testIndexOperations() throws Exception {
        final String clusterName = "index-ops-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_index_" + System.currentTimeMillis();

        // Test index existence before creation
        assertFalse(runner.indexExists(index));

        // Create index
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        assertTrue(runner.indexExists(index));

        // Close index
        runner.closeIndex(index);
        runner.ensureGreen();

        // Open index
        runner.openIndex(index);
        runner.ensureYellow(index);

        assertTrue(runner.indexExists(index));

        // Delete index
        runner.deleteIndex(index);

        assertFalse(runner.indexExists(index));
    }

    @Test
    public void testIndexCreationWithSettings() throws Exception {
        final String clusterName = "index-settings-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_index_with_settings";

        final Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

        final CreateIndexResponse response = runner.createIndex(index, indexSettings);
        assertTrue(response.isAcknowledged());

        runner.ensureYellow(index);
        assertTrue(runner.indexExists(index));

        runner.deleteIndex(index);
    }

    @Test
    public void testCreateIndexWithBuildCallback() throws Exception {
        final String clusterName = "index-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_index_callback";

        runner.createIndex(index, settingsBuilder -> {
            settingsBuilder.put("index.number_of_shards", 1);
            settingsBuilder.put("index.number_of_replicas", 0);
        });

        runner.ensureYellow(index);
        assertTrue(runner.indexExists(index));

        runner.deleteIndex(index);
    }

    @Test
    public void testSearchWithoutQuery() throws Exception {
        final String clusterName = "search-no-query-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_index";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Insert documents
        for (int i = 0; i < 10; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // Search without query (should return all)
        final var response = runner.search(index, null, null, 0, 5);
        assertEquals(10, response.getHits().getTotalHits().value());
        assertEquals(5, response.getHits().getHits().length);

        runner.deleteIndex(index);
    }

    @Test
    public void testSearchWithQuery() throws Exception {
        final String clusterName = "search-query-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_query_index";
        runner.createIndex(index, (Settings) null);

        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        runner.createMapping(index, mappingBuilder);
        runner.ensureYellow(index);

        // Insert documents
        runner.insert(index, "1", "{\"name\":\"Alice\"}");
        runner.insert(index, "2", "{\"name\":\"Bob\"}");
        runner.insert(index, "3", "{\"name\":\"Charlie\"}");
        runner.refresh();

        // Search with term query
        final var response = runner.search(index,
                QueryBuilders.termQuery("name", "Bob"),
                null, 0, 10);
        assertEquals(1, response.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testDocumentOperations() throws Exception {
        final String clusterName = "doc-ops-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_doc_ops";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Insert
        final IndexResponse insertResponse = runner.insert(index, "1",
                "{\"message\":\"Hello World\"}");
        assertNotNull(insertResponse);

        runner.refresh();

        // Count
        final var countResponse = runner.count(index);
        assertEquals(1, countResponse.getHits().getTotalHits().value());

        // Delete
        runner.delete(index, "1");
        runner.refresh();

        final var countAfterDelete = runner.count(index);
        assertEquals(0, countAfterDelete.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testFlushAndRefresh() throws Exception {
        final String clusterName = "flush-refresh-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_flush_refresh";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Insert document
        runner.insert(index, "1", "{\"value\":1}");

        // Flush
        runner.flush();

        // Refresh
        runner.refresh();

        // Search should find the document
        final var response = runner.count(index);
        assertEquals(1, response.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testClusterHealth() throws Exception {
        final String clusterName = "health-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));

        // Ensure yellow
        final ClusterHealthResponse yellowResponse = runner.ensureYellow();
        assertNotNull(yellowResponse);
        assertTrue(yellowResponse.getStatus() == ClusterHealthStatus.YELLOW ||
                   yellowResponse.getStatus() == ClusterHealthStatus.GREEN);

        // Ensure green (may not be achievable with 1 node and replicas)
        final ClusterHealthResponse greenResponse = runner.ensureGreen();
        assertNotNull(greenResponse);
    }

    @Test
    public void testNodeRetrieval() throws Exception {
        final String clusterName = "node-retrieval-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(2));
        runner.ensureYellow();

        // Test getNode by index
        assertNotNull(runner.getNode(0));
        assertNotNull(runner.getNode(1));
        assertNull(runner.getNode(2));
        assertNull(runner.getNode(-1));

        // Test getNode by name
        assertNotNull(runner.getNode("Node 1"));
        assertNotNull(runner.getNode("Node 2"));
        assertNull(runner.getNode("Node 3"));
        assertNull(runner.getNode("NonExistentNode"));

        // Test node()
        assertNotNull(runner.node());
    }

    @Test
    public void testGetInstance() throws Exception {
        final String clusterName = "getInstance-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final OpenSearchRunner instance = OpenSearchRunner.getInstance();
        assertNotNull(instance);
        assertEquals(runner, instance);
    }

    @Test
    public void testIsClosedMethod() throws Exception {
        final String clusterName = "isClosed-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertFalse(runner.isClosed());

        runner.close();

        assertTrue(runner.isClosed());
    }

    @Test
    public void testBasePath() throws Exception {
        final String clusterName = "basePath-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertNotNull(runner.basePath);
        assertTrue(Files.exists(Paths.get(runner.basePath)));

        runner.close();
        runner.clean();

        assertFalse(Files.exists(Paths.get(runner.basePath)));
    }

    @Test
    public void testConfigsBuilder() {
        final OpenSearchRunner.Configs configs = newConfigs()
                .clusterName("test-cluster")
                .numOfNode(3)
                .basePath("/tmp/test")
                .disableESLogger();

        assertNotNull(configs);
    }

    @Test
    public void testConfigsWithUseLogger() {
        final OpenSearchRunner.Configs configs = newConfigs()
                .clusterName("test-cluster")
                .numOfNode(1)
                .useLogger();

        assertNotNull(configs);
    }

    @Test
    public void testPrintOnFailure() throws Exception {
        final String clusterName = "print-fail-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // This should not throw an exception
        runner.print("Test message");

        runner.close();
    }

    @Test
    public void testMultipleMappings() throws Exception {
        final String clusterName = "multi-mapping-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_multi_mapping";
        runner.createIndex(index, (Settings) null);

        // Create first mapping
        final XContentBuilder mapping1 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("field1")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        runner.createMapping(index, mapping1);

        // Add more fields to mapping
        final XContentBuilder mapping2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("field2")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject();
        runner.createMapping(index, mapping2);

        runner.ensureYellow(index);

        // Insert document with both fields
        runner.insert(index, "1", "{\"field1\":\"value1\",\"field2\":\"value2\"}");
        runner.refresh();

        final var response = runner.count(index);
        assertEquals(1, response.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testClusterManagerAndNonClusterManagerNodes() throws Exception {
        final String clusterName = "master-node-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
            settingsBuilder.putList("cluster.initial_cluster_manager_nodes", "127.0.0.1:9301");
        }).build(newConfigs().clusterName(clusterName).numOfNode(2));
        runner.ensureYellow();

        assertNotNull(runner.clusterManagerNode());
        assertNotNull(runner.nonClusterManagerNode());
        assertNotEquals(runner.clusterManagerNode(), runner.nonClusterManagerNode());
    }

    @Test
    public void testWaitForRelocation() throws Exception {
        final String clusterName = "relocation-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Should complete without issues even if no relocation is happening
        runner.waitForRelocation();

        assertTrue(true); // If we get here, test passed
    }

    @Test
    public void testInsertWithSourceBuilder() throws Exception {
        final String clusterName = "insert-builder-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_insert_builder";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Test")
                .field("value", 123)
                .endObject();

        final IndexResponse response = runner.insert(index, "1", source);
        assertNotNull(response);

        runner.refresh();

        final var countResponse = runner.count(index);
        assertEquals(1, countResponse.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }
}

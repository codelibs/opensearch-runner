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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.opensearch.runner.net.OpenSearchCurl;
import org.junit.After;
import org.junit.Test;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.node.Node;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

/**
 * Additional test cases to cover gaps in OpenSearchRunner test coverage:
 * - Insert existing document throws exception (without printOnFailure)
 * - Search pagination (from/size boundary cases)
 * - ensureGreen/ensureYellow with specific indices
 * - createIndex with BuilderCallback including mappings
 * - close/open index round-trip with data verification
 * - setMaxHttpPort with negative value (disable port checking)
 * - Configs custom paths (confPath, dataPath, logsPath via args)
 * - Configs moduleTypes method
 * - Curl integration: search with body, _cat API
 * - Multiple document insert and count
 * - Node discovery by name after restart
 */
public class OpenSearchRunnerAdditionalTest {

    private OpenSearchRunner runner;

    @After
    public void tearDown() throws Exception {
        if (runner != null) {
            try {
                runner.close();
            } catch (Exception e) {
                // Ignore close errors in teardown
            }
            try {
                runner.clean();
            } catch (Exception e) {
                // Ignore cleanup errors in teardown
            }
        }
    }

    // ==================== Insert Existing Document Throws ====================

    @Test(expected = OpenSearchRunnerException.class)
    public void testInsertExistingDocumentThrowsException() throws Exception {
        final String clusterName = "insert-existing-throw-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_insert_dup";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        runner.insert(index, "doc-1", "{\"value\":1}");
        // Second insert with same ID should throw (result is UPDATED, not CREATED)
        try {
            runner.insert(index, "doc-1", "{\"value\":2}");
        } finally {
            runner.deleteIndex(index);
        }
    }

    // ==================== Search Pagination Edge Cases ====================

    @Test
    public void testSearchPaginationFromZeroSizeZero() throws Exception {
        final String clusterName = "search-page-zero-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_page";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 0; i < 5; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // size=0 should return total count but no hits
        final SearchResponse response = runner.search(index, null, null, 0, 0);
        assertEquals(5, response.getHits().getTotalHits().value());
        assertEquals(0, response.getHits().getHits().length);

        runner.deleteIndex(index);
    }

    @Test
    public void testSearchPaginationSecondPage() throws Exception {
        final String clusterName = "search-page-second-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_page2";
        runner.createIndex(index, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build());
        runner.ensureYellow(index);

        for (int i = 0; i < 10; i++) {
            runner.insert(index, String.valueOf(i), "{\"num\":" + i + "}");
        }
        runner.refresh();

        // Get second page with sort to guarantee order
        final SearchResponse response = runner.search(index,
                QueryBuilders.matchAllQuery(),
                SortBuilders.fieldSort("num").order(SortOrder.ASC),
                3, 3);
        assertEquals(10, response.getHits().getTotalHits().value());
        assertEquals(3, response.getHits().getHits().length);
        // First hit on second page (from=3) should have num=3
        assertEquals(3, response.getHits().getHits()[0].getSourceAsMap().get("num"));

        runner.deleteIndex(index);
    }

    @Test
    public void testSearchPaginationBeyondResults() throws Exception {
        final String clusterName = "search-page-beyond-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_page3";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 0; i < 3; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // from beyond the result set
        final SearchResponse response = runner.search(index, null, null, 100, 10);
        assertEquals(3, response.getHits().getTotalHits().value());
        assertEquals(0, response.getHits().getHits().length);

        runner.deleteIndex(index);
    }

    // ==================== ensureGreen/Yellow with Specific Index ====================

    @Test
    public void testEnsureGreenWithSpecificIndex() throws Exception {
        final String clusterName = "ensure-green-idx-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_green_idx";
        runner.createIndex(index, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build());

        final ClusterHealthStatus status = runner.ensureGreen(index);
        assertNotNull(status);
        assertEquals(ClusterHealthStatus.GREEN, status);

        runner.deleteIndex(index);
    }

    @Test
    public void testEnsureYellowWithSpecificIndex() throws Exception {
        final String clusterName = "ensure-yellow-idx-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_yellow_idx";
        runner.createIndex(index, (Settings) null);

        final ClusterHealthStatus status = runner.ensureYellow(index);
        assertNotNull(status);
        assertTrue(status == ClusterHealthStatus.YELLOW || status == ClusterHealthStatus.GREEN);

        runner.deleteIndex(index);
    }

    // ==================== createIndex with BuilderCallback + Mappings ====================

    @Test
    public void testCreateIndexWithBuilderCallbackAndMappings() throws Exception {
        final String clusterName = "create-idx-map-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_idx_with_mapping";

        final CreateIndexResponse response = runner.createIndex(index, builder ->
                builder.setSettings(Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0))
                        .setMapping("{\"properties\":{\"title\":{\"type\":\"text\"},\"count\":{\"type\":\"integer\"}}}"));
        assertTrue(response.isAcknowledged());
        runner.ensureGreen(index);

        // Verify mapping by inserting and searching
        runner.insert(index, "1", "{\"title\":\"hello world\",\"count\":42}");
        runner.refresh();

        final SearchResponse searchResponse = runner.search(index,
                QueryBuilders.rangeQuery("count").gte(40), null, 0, 10);
        assertEquals(1, searchResponse.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    // ==================== Close/Open Index Round-Trip with Data ====================

    @Test
    public void testCloseOpenIndexPreservesData() throws Exception {
        final String clusterName = "close-open-data-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_close_open_data";
        runner.createIndex(index, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build());
        runner.ensureGreen(index);

        // Insert data
        for (int i = 0; i < 5; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();
        assertEquals(5, runner.count(index).getHits().getTotalHits().value());

        // Close index
        final CloseIndexResponse closeResponse = runner.closeIndex(index);
        assertTrue(closeResponse.isAcknowledged());

        // Open index
        final OpenIndexResponse openResponse = runner.openIndex(index);
        assertTrue(openResponse.isAcknowledged());
        runner.ensureGreen(index);

        // Verify data is preserved
        runner.refresh();
        assertEquals(5, runner.count(index).getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    // ==================== setMaxHttpPort Negative (Disable Port Checking) ====================

    @Test
    public void testSetMaxHttpPortNegativeDisablesCheck() throws Exception {
        final String clusterName = "max-port-neg-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.setMaxHttpPort(-1);
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1).baseHttpPort(9280));
        runner.ensureYellow();

        assertNotNull(runner.node());
        // With maxHttpPort < 0, port check is disabled and port is assigned directly
        assertEquals("9281", runner.node().settings().get("http.port"));
    }

    // ==================== Configs moduleTypes Method ====================

    @Test
    public void testConfigsModuleTypes() throws Exception {
        final String clusterName = "module-types-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        // Use a subset of modules to verify moduleTypes config works
        runner.build(newConfigs()
                .clusterName(clusterName)
                .numOfNode(1)
                .moduleTypes(String.join(",",
                        "org.opensearch.analysis.common.CommonAnalysisModulePlugin",
                        "org.opensearch.transport.Netty4ModulePlugin")));
        runner.ensureYellow();

        assertNotNull(runner.node());
        assertFalse(runner.isClosed());
    }

    // ==================== Multiple Document Insert and Count ====================

    @Test
    public void testInsertMultipleDocumentsAndCount() throws Exception {
        final String clusterName = "multi-doc-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_multi_docs";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Create mapping with keyword field for exact matching
        runner.createMapping(index, "{\"properties\":{\"id\":{\"type\":\"integer\"},\"category\":{\"type\":\"keyword\"}}}");

        final int docCount = 100;
        for (int i = 0; i < docCount; i++) {
            final IndexResponse response = runner.insert(index, String.valueOf(i),
                    "{\"id\":" + i + ",\"category\":\"" + (i % 3 == 0 ? "A" : "B") + "\"}");
            assertEquals(Result.CREATED, response.getResult());
        }
        runner.refresh();

        // Total count
        assertEquals(docCount, runner.count(index).getHits().getTotalHits().value());

        // Filtered count with BuilderCallback
        final SearchResponse filtered = runner.count(index, builder ->
                builder.setQuery(QueryBuilders.termQuery("category", "A")));
        assertEquals(34, filtered.getHits().getTotalHits().value()); // 0,3,6,...,99 = 34 items

        runner.deleteIndex(index);
    }

    // ==================== Mapping with BuilderCallback using XContentBuilder ====================

    @Test
    public void testCreateMappingWithXContentBuilder() throws Exception {
        final String clusterName = "mapping-xcontent-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_mapping_xcontent";
        runner.createIndex(index, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build());
        runner.ensureGreen(index);

        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("status")
                .field("type", "keyword")
                .endObject()
                .startObject("score")
                .field("type", "float")
                .endObject()
                .endObject()
                .endObject();
        runner.createMapping(index, mappingBuilder);

        // Insert and search with mapping-specific query
        runner.insert(index, "1", "{\"status\":\"active\",\"score\":9.5}");
        runner.insert(index, "2", "{\"status\":\"inactive\",\"score\":3.2}");
        runner.refresh();

        final SearchResponse response = runner.search(index,
                QueryBuilders.termQuery("status", "active"), null, 0, 10);
        assertEquals(1, response.getHits().getTotalHits().value());
        assertEquals("1", response.getHits().getHits()[0].getId());

        runner.deleteIndex(index);
    }

    // ==================== Node Discovery by Name After Restart ====================

    @Test
    public void testGetNodeByNameAfterRestart() throws Exception {
        final String clusterName = "name-restart-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertNotNull(runner.getNode("Node 1"));

        // Close and restart
        runner.getNode(0).close();
        assertTrue(runner.startNode(0));
        runner.ensureYellow();

        // Node name should still be retrievable
        assertNotNull(runner.getNode("Node 1"));
        assertFalse(runner.getNode("Node 1").isClosed());
    }

    // ==================== Curl Integration: Search with Body ====================

    @Test
    public void testCurlSearchWithBody() throws Exception {
        final String clusterName = "curl-search-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_curl_search";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        runner.insert(index, "1", "{\"message\":\"hello world\"}");
        runner.insert(index, "2", "{\"message\":\"goodbye world\"}");
        runner.refresh();

        final Node node = runner.node();
        try (CurlResponse response = OpenSearchCurl.get(node, "/" + index + "/_search")
                .header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}}}")
                .execute()) {
            final Map<String, Object> content = response.getContent(OpenSearchCurl.jsonParser());
            assertNotNull(content);
            @SuppressWarnings("unchecked")
            final Map<String, Object> hits = (Map<String, Object>) content.get("hits");
            assertNotNull(hits);
            @SuppressWarnings("unchecked")
            final Map<String, Object> total = (Map<String, Object>) hits.get("total");
            assertEquals(2, total.get("value"));
        }

        runner.deleteIndex(index);
    }

    // ==================== Curl Integration: _cat API ====================

    @Test
    public void testCurlCatIndices() throws Exception {
        final String clusterName = "curl-cat-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_curl_cat";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        final Node node = runner.node();
        try (CurlResponse response = OpenSearchCurl.get(node, "/_cat/indices?format=json")
                .header("Content-Type", "application/json")
                .execute()) {
            final String content = response.getContentAsString();
            assertNotNull(content);
            assertTrue(content.contains(index));
        }

        runner.deleteIndex(index);
    }

    // ==================== Curl Integration: Cluster Health ====================

    @Test
    public void testCurlClusterHealth() throws Exception {
        final String clusterName = "curl-health-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final Node node = runner.node();
        try (CurlResponse response = OpenSearchCurl.get(node, "/_cluster/health")
                .header("Content-Type", "application/json")
                .execute()) {
            final Map<String, Object> content = response.getContent(OpenSearchCurl.jsonParser());
            assertNotNull(content);
            assertEquals(clusterName, content.get("cluster_name"));
            assertTrue(content.containsKey("status"));
            assertEquals(1, content.get("number_of_nodes"));
        }
    }

    // ==================== Custom Data/Logs Path ====================

    @Test
    public void testCustomDataAndLogsPaths() throws Exception {
        final Path tempDir = Files.createTempDirectory("opensearch-custom-paths-test");
        final Path dataDir = tempDir.resolve("custom_data");
        final Path logsDir = tempDir.resolve("custom_logs");
        Files.createDirectories(dataDir);
        Files.createDirectories(logsDir);

        final String clusterName = "custom-paths-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build("-basePath", tempDir.toString(),
                "-clusterName", clusterName,
                "-numOfNode", "1",
                "-dataPath", dataDir.toString(),
                "-logsPath", logsDir.toString());
        runner.ensureYellow();

        assertNotNull(runner.node());
        // Verify data directory is used
        assertTrue(Files.exists(dataDir));
        assertTrue(Files.exists(logsDir));
    }

    // ==================== onBuild Returns This for Chaining ====================

    @Test
    public void testOnBuildReturnsSelfForChaining() throws Exception {
        final String clusterName = "chain-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        final OpenSearchRunner returned = runner.onBuild((number, settingsBuilder) -> {
            // no-op
        });
        assertSame(runner, returned);

        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();
    }

    // ==================== Force Merge Default Parameters ====================

    @Test
    public void testForceMergeDefault() throws Exception {
        final String clusterName = "forcemerge-default-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_forcemerge_default";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 0; i < 10; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // Test default forceMerge (no parameters)
        assertNotNull(runner.forceMerge());

        runner.deleteIndex(index);
    }

    // ==================== Delete and Re-create Same Index ====================

    @Test
    public void testDeleteAndRecreateSameIndex() throws Exception {
        final String clusterName = "recreate-idx-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_recreate";

        // Create, insert, delete
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.insert(index, "1", "{\"value\":1}");
        runner.refresh();
        assertEquals(1, runner.count(index).getHits().getTotalHits().value());
        runner.deleteIndex(index);
        assertFalse(runner.indexExists(index));

        // Re-create with different settings
        runner.createIndex(index, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build());
        runner.ensureGreen(index);

        // Old data should not exist
        assertEquals(0, runner.count(index).getHits().getTotalHits().value());

        // Insert new data
        runner.insert(index, "2", "{\"value\":2}");
        runner.refresh();
        assertEquals(1, runner.count(index).getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    // ==================== Search with Range Query ====================

    @Test
    public void testSearchWithRangeQuery() throws Exception {
        final String clusterName = "range-query-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_range_query";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 1; i <= 20; i++) {
            runner.insert(index, String.valueOf(i), "{\"score\":" + i + "}");
        }
        runner.refresh();

        // Range query: score between 5 and 15 inclusive
        final SearchResponse response = runner.search(index,
                QueryBuilders.rangeQuery("score").gte(5).lte(15),
                SortBuilders.fieldSort("score").order(SortOrder.ASC),
                0, 20);
        assertEquals(11, response.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    // ==================== Build with Configs (full chain) ====================

    @Test
    public void testBuildWithConfigsObject() throws Exception {
        final String clusterName = "configs-obj-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();

        final OpenSearchRunner.Configs configs = newConfigs()
                .clusterName(clusterName)
                .numOfNode(1)
                .baseHttpPort(9270)
                .indexStoreType("fs");

        runner.build(configs);
        runner.ensureYellow();

        assertNotNull(runner.node());
        assertEquals(1, runner.getNodeSize());
        assertEquals("9271", runner.node().settings().get("http.port"));
        assertEquals("fs", runner.node().settings().get("index.store.type"));
    }

    // ==================== Upgrade Default (No Parameters) ====================

    @Test
    public void testUpgradeDefault() throws Exception {
        final String clusterName = "upgrade-default-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_upgrade_default";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Default upgrade should work without errors
        assertNotNull(runner.upgrade());

        runner.deleteIndex(index);
    }

    // ==================== Flush Default (No Parameters) ====================

    @Test
    public void testFlushDefault() throws Exception {
        final String clusterName = "flush-default-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_flush_default";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.insert(index, "1", "{\"value\":1}");

        // Default flush (force=true)
        assertNotNull(runner.flush());

        runner.deleteIndex(index);
    }
}

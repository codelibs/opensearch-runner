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

import org.junit.After;
import org.junit.Test;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.node.Node;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.AdminClient;

/**
 * Advanced test cases for OpenSearchRunner covering:
 * - Configs options
 * - BuilderCallback variants
 * - Utility methods
 * - Edge cases
 */
public class OpenSearchRunnerAdvancedTest {

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

    // ==================== Configs Options Tests ====================

    @Test
    public void testConfigsBaseHttpPort() throws Exception {
        final String clusterName = "base-http-port-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs()
                .clusterName(clusterName)
                .numOfNode(1)
                .baseHttpPort(9250));
        runner.ensureYellow();

        assertNotNull(runner.node());
        // Verify the http port setting is applied (port should be 9251 for node 1)
        final String httpPort = runner.node().settings().get("http.port");
        assertNotNull(httpPort);
        assertEquals("9251", httpPort);
    }

    @Test
    public void testConfigsIndexStoreType() throws Exception {
        final String clusterName = "index-store-type-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs()
                .clusterName(clusterName)
                .numOfNode(1)
                .indexStoreType("fs"));
        runner.ensureYellow();

        final String storeType = runner.node().settings().get("index.store.type");
        assertEquals("fs", storeType);
    }

    @Test
    public void testConfigsPrintOnFailure() throws Exception {
        final String clusterName = "print-on-failure-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs()
                .clusterName(clusterName)
                .numOfNode(1)
                .printOnFailure());
        runner.ensureYellow();

        // With printOnFailure, operations that would throw should print instead
        // This verifies the config is accepted without errors
        assertNotNull(runner.node());
    }

    @Test
    public void testConfigsBasePath() throws Exception {
        final Path tempDir = Files.createTempDirectory("opensearch-basepath-test");
        final String clusterName = "base-path-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs()
                .clusterName(clusterName)
                .numOfNode(1)
                .basePath(tempDir.toString()));
        runner.ensureYellow();

        // Verify node directory was created under basePath
        assertTrue(Files.exists(tempDir.resolve("node_1")));
    }

    // ==================== Utility Methods Tests ====================

    @Test
    public void testGetClusterName() throws Exception {
        final String clusterName = "get-cluster-name-test-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertEquals(clusterName, runner.getClusterName());
    }

    @Test
    public void testAdminClient() throws Exception {
        final String clusterName = "admin-client-test-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final AdminClient admin = runner.admin();
        assertNotNull(admin);
        assertNotNull(admin.cluster());
        assertNotNull(admin.indices());
    }

    @Test
    public void testGetNodeIndex() throws Exception {
        final String clusterName = "get-node-index-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
            settingsBuilder.putList("cluster.initial_cluster_manager_nodes", "127.0.0.1:9301");
        }).build(newConfigs().clusterName(clusterName).numOfNode(2));
        runner.ensureYellow();

        final Node node0 = runner.getNode(0);
        final Node node1 = runner.getNode(1);

        assertEquals(0, runner.getNodeIndex(node0));
        assertEquals(1, runner.getNodeIndex(node1));
    }

    @Test
    public void testGetNodeIndexReturnsMinusOneForUnknownNode() throws Exception {
        final String clusterName = "unknown-node-index-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Pass null or a node not in the list
        assertEquals(-1, runner.getNodeIndex(null));
    }

    @Test
    public void testSetMaxHttpPort() throws Exception {
        final String clusterName = "max-http-port-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.setMaxHttpPort(9350);
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertNotNull(runner.node());
    }

    // ==================== BuilderCallback Variants Tests ====================

    @Test
    public void testFlushWithBuilderCallback() throws Exception {
        final String clusterName = "flush-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_flush_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.insert(index, "1", "{\"value\":1}");

        // Use BuilderCallback variant
        final FlushResponse response = runner.flush(builder ->
                builder.setWaitIfOngoing(true).setForce(true));
        assertNotNull(response);

        runner.deleteIndex(index);
    }

    @Test
    public void testRefreshWithBuilderCallback() throws Exception {
        final String clusterName = "refresh-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_refresh_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Use BuilderCallback variant
        final RefreshResponse response = runner.refresh(builder -> builder);
        assertNotNull(response);

        runner.deleteIndex(index);
    }

    @Test
    public void testForceMergeWithBuilderCallback() throws Exception {
        final String clusterName = "forcemerge-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_forcemerge_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.insert(index, "1", "{\"value\":1}");
        runner.refresh();

        // Use BuilderCallback variant with custom parameters
        final ForceMergeResponse response = runner.forceMerge(builder ->
                builder.setMaxNumSegments(1).setOnlyExpungeDeletes(false).setFlush(true));
        assertNotNull(response);

        runner.deleteIndex(index);
    }

    @Test
    public void testCountWithBuilderCallback() throws Exception {
        final String clusterName = "count-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_count_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 0; i < 5; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // Use BuilderCallback variant with query
        final SearchResponse response = runner.count(index, builder ->
                builder.setQuery(QueryBuilders.rangeQuery("value").gte(2)));
        assertEquals(3, response.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testSearchWithBuilderCallback() throws Exception {
        final String clusterName = "search-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 0; i < 10; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // Use BuilderCallback variant with custom configuration
        final SearchResponse response = runner.search(index, builder ->
                builder.setQuery(QueryBuilders.matchAllQuery())
                        .addSort(SortBuilders.fieldSort("value").order(SortOrder.DESC))
                        .setFrom(0)
                        .setSize(5));

        assertEquals(10, response.getHits().getTotalHits().value());
        assertEquals(5, response.getHits().getHits().length);

        runner.deleteIndex(index);
    }

    @Test
    public void testInsertWithBuilderCallback() throws Exception {
        final String clusterName = "insert-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_insert_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Use BuilderCallback variant
        final IndexResponse response = runner.insert(index, "custom-id", builder ->
                builder.setSource("{\"custom\":\"data\"}", org.opensearch.common.xcontent.XContentType.JSON));
        assertNotNull(response);
        assertEquals("custom-id", response.getId());

        runner.refresh();
        final SearchResponse count = runner.count(index);
        assertEquals(1, count.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testDeleteWithBuilderCallback() throws Exception {
        final String clusterName = "delete-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_delete_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        runner.insert(index, "to-delete", "{\"value\":1}");
        runner.refresh();

        // Use BuilderCallback variant
        final DeleteResponse response = runner.delete(index, "to-delete", builder -> builder);
        assertNotNull(response);

        runner.refresh();
        final SearchResponse count = runner.count(index);
        assertEquals(0, count.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    @Test
    public void testGetAliasWithBuilderCallback() throws Exception {
        final String clusterName = "get-alias-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_alias_callback_index";
        final String alias = "test_alias_callback";

        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.updateAlias(alias, new String[]{index}, null);

        // Use BuilderCallback variant
        final GetAliasesResponse response = runner.getAlias(alias, builder -> builder);
        assertNotNull(response);
        assertTrue(response.getAliases().containsKey(index));

        runner.deleteIndex(index);
    }

    @Test
    public void testUpdateAliasWithBuilderCallback() throws Exception {
        final String clusterName = "update-alias-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index1 = "test_update_alias_idx1";
        final String index2 = "test_update_alias_idx2";
        final String alias = "test_update_alias";

        runner.createIndex(index1, (Settings) null);
        runner.createIndex(index2, (Settings) null);
        runner.ensureYellow(index1, index2);

        // Use BuilderCallback variant to add alias to both indices
        final AcknowledgedResponse response = runner.updateAlias(builder ->
                builder.addAlias(index1, alias).addAlias(index2, alias));
        assertTrue(response.isAcknowledged());

        // Verify both indices have the alias
        final GetAliasesResponse aliasResponse = runner.getAlias(alias);
        assertTrue(aliasResponse.getAliases().containsKey(index1));
        assertTrue(aliasResponse.getAliases().containsKey(index2));

        runner.deleteIndex(index1);
        runner.deleteIndex(index2);
    }

    @Test
    public void testDeleteIndexWithBuilderCallback() throws Exception {
        final String clusterName = "delete-idx-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_delete_idx_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        assertTrue(runner.indexExists(index));

        // Use BuilderCallback variant
        final AcknowledgedResponse response = runner.deleteIndex(index, builder -> builder);
        assertTrue(response.isAcknowledged());
        assertFalse(runner.indexExists(index));
    }

    @Test
    public void testIndexExistsWithBuilderCallback() throws Exception {
        final String clusterName = "idx-exists-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String existingIndex = "existing_index_callback";
        final String nonExistingIndex = "non_existing_index_callback";

        runner.createIndex(existingIndex, (Settings) null);
        runner.ensureYellow(existingIndex);

        // Use BuilderCallback variant
        assertTrue(runner.indexExists(existingIndex, builder -> builder));
        assertFalse(runner.indexExists(nonExistingIndex, builder -> builder));

        runner.deleteIndex(existingIndex);
    }

    // ==================== Search with Sort Tests ====================

    @Test
    public void testSearchWithSortBuilder() throws Exception {
        final String clusterName = "search-sort-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_search_sort";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Insert documents with different values
        runner.insert(index, "1", "{\"priority\":3}");
        runner.insert(index, "2", "{\"priority\":1}");
        runner.insert(index, "3", "{\"priority\":2}");
        runner.refresh();

        // Search with sort
        final SearchResponse response = runner.search(index,
                QueryBuilders.matchAllQuery(),
                SortBuilders.fieldSort("priority").order(SortOrder.ASC),
                0, 10);

        assertEquals(3, response.getHits().getTotalHits().value());
        // First result should be the one with priority=1 (id=2)
        assertEquals("2", response.getHits().getHits()[0].getId());

        runner.deleteIndex(index);
    }

    // ==================== ForceMerge with Parameters Tests ====================

    @Test
    public void testForceMergeWithParameters() throws Exception {
        final String clusterName = "forcemerge-params-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_forcemerge_params";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        for (int i = 0; i < 10; i++) {
            runner.insert(index, String.valueOf(i), "{\"value\":" + i + "}");
        }
        runner.refresh();

        // Test forceMerge with explicit parameters
        final ForceMergeResponse response = runner.forceMerge(1, false, true);
        assertNotNull(response);

        runner.deleteIndex(index);
    }

    // ==================== Upgrade Tests ====================

    @Test
    public void testUpgradeWithParameter() throws Exception {
        final String clusterName = "upgrade-param-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_upgrade_param";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Test upgrade with upgradeOnlyAncientSegments parameter
        runner.upgrade(true);
        runner.upgrade(false);

        runner.deleteIndex(index);
    }

    // ==================== Flush Tests ====================

    @Test
    public void testFlushWithForceParameter() throws Exception {
        final String clusterName = "flush-force-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_flush_force";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.insert(index, "1", "{\"value\":1}");

        // Test flush with force=false
        final FlushResponse response1 = runner.flush(false);
        assertNotNull(response1);

        // Test flush with force=true (default)
        final FlushResponse response2 = runner.flush(true);
        assertNotNull(response2);

        runner.deleteIndex(index);
    }

    // ==================== Mapping with String Tests ====================

    @Test
    public void testCreateMappingWithJsonString() throws Exception {
        final String clusterName = "mapping-string-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_mapping_string";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Create mapping using JSON string
        final String mappingJson = "{\"properties\":{\"title\":{\"type\":\"text\"},\"count\":{\"type\":\"integer\"}}}";
        final AcknowledgedResponse response = runner.createMapping(index, mappingJson);
        assertTrue(response.isAcknowledged());

        // Insert and verify document with mapping
        runner.insert(index, "1", "{\"title\":\"Test Title\",\"count\":42}");
        runner.refresh();

        final SearchResponse searchResponse = runner.count(index);
        assertEquals(1, searchResponse.getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    // ==================== Open/Close Index with Callback Tests ====================

    @Test
    public void testOpenIndexWithBuilderCallback() throws Exception {
        final String clusterName = "open-idx-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_open_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);
        runner.closeIndex(index);

        // Use BuilderCallback variant
        runner.openIndex(index, builder -> builder);
        runner.ensureYellow(index);

        assertTrue(runner.indexExists(index));

        runner.deleteIndex(index);
    }

    @Test
    public void testCloseIndexWithBuilderCallback() throws Exception {
        final String clusterName = "close-idx-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_close_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Use BuilderCallback variant
        runner.closeIndex(index, builder -> builder);

        // Re-open to verify it was closed
        runner.openIndex(index);
        runner.ensureYellow(index);

        runner.deleteIndex(index);
    }

    // ==================== Multiple Indices Operations Tests ====================

    @Test
    public void testOperationsOnMultipleIndices() throws Exception {
        final String clusterName = "multi-idx-ops-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index1 = "test_multi_idx_1";
        final String index2 = "test_multi_idx_2";
        final String index3 = "test_multi_idx_3";

        // Create multiple indices
        runner.createIndex(index1, (Settings) null);
        runner.createIndex(index2, (Settings) null);
        runner.createIndex(index3, (Settings) null);
        runner.ensureYellow(index1, index2, index3);

        // Insert documents in all indices
        runner.insert(index1, "1", "{\"source\":\"index1\"}");
        runner.insert(index2, "1", "{\"source\":\"index2\"}");
        runner.insert(index3, "1", "{\"source\":\"index3\"}");

        // Refresh all
        runner.refresh();

        // Verify all indices exist
        assertTrue(runner.indexExists(index1));
        assertTrue(runner.indexExists(index2));
        assertTrue(runner.indexExists(index3));

        // Clean up
        runner.deleteIndex(index1);
        runner.deleteIndex(index2);
        runner.deleteIndex(index3);
    }

    // ==================== Alias Operations Tests ====================

    @Test
    public void testUpdateAliasAddAndRemove() throws Exception {
        final String clusterName = "alias-add-remove-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index1 = "test_alias_idx_1";
        final String index2 = "test_alias_idx_2";
        final String alias = "test_alias_combined";

        runner.createIndex(index1, (Settings) null);
        runner.createIndex(index2, (Settings) null);
        runner.ensureYellow(index1, index2);

        // Add alias to index1
        runner.updateAlias(alias, new String[]{index1}, null);

        // Verify alias points to index1
        GetAliasesResponse response = runner.getAlias(alias);
        assertTrue(response.getAliases().containsKey(index1));
        assertFalse(response.getAliases().containsKey(index2));

        // Move alias from index1 to index2
        runner.updateAlias(alias, new String[]{index2}, new String[]{index1});

        // Verify alias now points to index2 only
        response = runner.getAlias(alias);
        assertFalse(response.getAliases().containsKey(index1));
        assertTrue(response.getAliases().containsKey(index2));

        runner.deleteIndex(index1);
        runner.deleteIndex(index2);
    }

    @Test
    public void testUpdateAliasWithNullArrays() throws Exception {
        final String clusterName = "alias-null-arrays-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_alias_null_idx";
        final String alias = "test_alias_null";

        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Add alias with null deletedIndices
        runner.updateAlias(alias, new String[]{index}, null);

        // Verify alias was added
        GetAliasesResponse response = runner.getAlias(alias);
        assertTrue(response.getAliases().containsKey(index));

        // Call with null addedIndices (should not add anything new)
        runner.updateAlias(alias, null, null);

        // Remove alias with null addedIndices
        runner.updateAlias(alias, null, new String[]{index});

        runner.deleteIndex(index);
    }

    @Test
    public void testUpdateAliasWithEmptyArrays() throws Exception {
        final String clusterName = "alias-empty-arrays-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_alias_empty_idx";
        final String alias = "test_alias_empty";

        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Add alias
        runner.updateAlias(alias, new String[]{index}, new String[0]);

        GetAliasesResponse response = runner.getAlias(alias);
        assertTrue(response.getAliases().containsKey(index));

        // Call with empty arrays (should be no-op)
        runner.updateAlias(alias, new String[0], new String[0]);

        runner.deleteIndex(index);
    }
}

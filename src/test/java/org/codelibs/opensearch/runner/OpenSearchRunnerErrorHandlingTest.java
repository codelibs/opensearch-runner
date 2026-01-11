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

import org.junit.After;
import org.junit.Test;
import org.opensearch.common.settings.Settings;

/**
 * Test cases for error handling and edge cases in OpenSearchRunner.
 */
public class OpenSearchRunnerErrorHandlingTest {

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

    // ==================== Node Access Edge Cases ====================

    @Test
    public void testGetNodeWithNegativeIndex() throws Exception {
        final String clusterName = "negative-idx-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertNull(runner.getNode(-1));
        assertNull(runner.getNode(-100));
    }

    @Test
    public void testGetNodeWithIndexBeyondSize() throws Exception {
        final String clusterName = "beyond-size-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(2));
        runner.ensureYellow();

        assertNull(runner.getNode(2));
        assertNull(runner.getNode(100));
    }

    @Test
    public void testGetNodeByNullName() throws Exception {
        final String clusterName = "null-name-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertNull(runner.getNode((String) null));
    }

    @Test
    public void testGetNodeByNonExistentName() throws Exception {
        final String clusterName = "non-existent-name-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        assertNull(runner.getNode("NonExistentNodeName"));
        assertNull(runner.getNode(""));
        assertNull(runner.getNode("Node 999"));
    }

    // ==================== All Nodes Closed Scenario ====================

    @Test
    public void testNodeMethodThrowsWhenAllNodesClosed() throws Exception {
        final String clusterName = "all-closed-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Close all nodes
        runner.close();

        // Now calling node() should throw
        try {
            runner.node();
            fail("Expected OpenSearchRunnerException when all nodes are closed");
        } catch (OpenSearchRunnerException e) {
            assertEquals("All nodes are closed.", e.getMessage());
        }
    }

    // ==================== startNode Edge Cases ====================

    @Test
    public void testStartNodeWithInvalidIndex() throws Exception {
        final String clusterName = "invalid-start-idx-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Try to start a node at an invalid index
        assertFalse(runner.startNode(5));
        assertFalse(runner.startNode(100));
    }

    @Test
    public void testStartNodeWhenNotClosed() throws Exception {
        final String clusterName = "not-closed-start-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Node is not closed, so startNode should return false
        assertFalse(runner.startNode(0));
    }

    @Test
    public void testStartNodeAfterClose() throws Exception {
        final String clusterName = "start-after-close-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Close the node
        runner.getNode(0).close();
        assertTrue(runner.getNode(0).isClosed());

        // Start it again
        assertTrue(runner.startNode(0));
        runner.ensureYellow();

        assertFalse(runner.node().isClosed());
    }

    // ==================== Index Operations Error Cases ====================

    @Test
    public void testDeleteNonExistentDocument() throws Exception {
        final String clusterName = "delete-nonexistent-doc-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1).printOnFailure());
        runner.ensureYellow();

        final String index = "test_delete_nonexistent";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // With printOnFailure, deleting non-existent document should not throw
        // It will print instead
        runner.delete(index, "non-existent-id");

        runner.deleteIndex(index);
    }

    @Test(expected = OpenSearchRunnerException.class)
    public void testDeleteNonExistentDocumentThrows() throws Exception {
        final String clusterName = "delete-nonexistent-throw-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_delete_throw";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Without printOnFailure, should throw exception
        try {
            runner.delete(index, "non-existent-id");
        } finally {
            runner.deleteIndex(index);
        }
    }

    // ==================== Closed State Tests ====================

    @Test
    public void testIsClosedWithAllNodesClosed() throws Exception {
        final String clusterName = "is-closed-all-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(2));
        runner.ensureYellow();

        assertFalse(runner.isClosed());

        // Close first node only
        runner.getNode(0).close();
        assertFalse(runner.isClosed()); // Still one node running

        // Close second node
        runner.getNode(1).close();
        assertTrue(runner.isClosed()); // All nodes closed
    }

    @Test
    public void testIsClosedWithEmptyNodeList() {
        runner = new OpenSearchRunner();
        // No nodes built yet
        assertTrue(runner.isClosed());
    }

    // ==================== Search on Non-Existent Index ====================

    @Test(expected = Exception.class)
    public void testSearchOnNonExistentIndex() throws Exception {
        final String clusterName = "search-nonexistent-idx-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Should throw because index doesn't exist
        runner.search("non_existent_index", null, null, 0, 10);
    }

    // ==================== Insert Result Not Created ====================

    @Test
    public void testInsertExistingDocumentWithPrintOnFailure() throws Exception {
        final String clusterName = "insert-existing-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1).printOnFailure());
        runner.ensureYellow();

        final String index = "test_insert_existing";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Insert first document
        runner.insert(index, "doc-1", "{\"value\":1}");
        runner.refresh();

        // Insert again with same ID - with printOnFailure should not throw
        // but will print instead (result is UPDATED, not CREATED)
        runner.insert(index, "doc-1", "{\"value\":2}");

        runner.deleteIndex(index);
    }

    // ==================== nonClusterManagerNode Returns Null ====================

    @Test
    public void testNonClusterManagerNodeWithSingleNode() throws Exception {
        final String clusterName = "single-node-non-cm-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // With only one node, it must be the cluster manager
        // So nonClusterManagerNode should return null
        assertNull(runner.nonClusterManagerNode());
    }

    // ==================== Print Method ====================

    @Test
    public void testPrintWithLogger() throws Exception {
        final String clusterName = "print-logger-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1).useLogger());
        runner.ensureYellow();

        // Should not throw
        runner.print("Test log message with logger");
    }

    @Test
    public void testPrintWithoutLogger() throws Exception {
        final String clusterName = "print-no-logger-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Should not throw
        runner.print("Test message without logger");
    }

    // ==================== Mapping Builder Callback ====================

    @Test
    public void testCreateMappingWithBuilderCallback() throws Exception {
        final String clusterName = "mapping-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_mapping_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Use BuilderCallback variant
        runner.createMapping(index, builder ->
                builder.setSource("{\"properties\":{\"field1\":{\"type\":\"keyword\"}}}",
                        org.opensearch.common.xcontent.XContentType.JSON));

        runner.insert(index, "1", "{\"field1\":\"value1\"}");
        runner.refresh();

        assertEquals(1, runner.count(index).getHits().getTotalHits().value());

        runner.deleteIndex(index);
    }

    // ==================== Build with Null Args ====================

    @Test
    public void testBuildWithNullArgs() throws Exception {
        runner = new OpenSearchRunner();
        // Should use defaults when null is passed
        runner.build((String[]) null);
        runner.ensureYellow();

        assertNotNull(runner.node());
        assertEquals(3, runner.getNodeSize()); // default numOfNode is 3
    }

    // ==================== Upgrade with BuilderCallback ====================

    @Test
    public void testUpgradeWithBuilderCallback() throws Exception {
        final String clusterName = "upgrade-callback-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final String index = "test_upgrade_callback";
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // Use BuilderCallback variant
        runner.upgrade(builder -> builder.setUpgradeOnlyAncientSegments(false));

        runner.deleteIndex(index);
    }

    // ==================== Exception Response Retrieval ====================

    @Test
    public void testOpenSearchRunnerExceptionWithResponse() {
        // Create a mock scenario where we can verify exception contains response
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException("Test failure message");
        assertNull(exception.getActionResponse());
        assertEquals("Test failure message", exception.getMessage());
    }

    // ==================== Clean After Close ====================

    @Test
    public void testCleanAfterClose() throws Exception {
        final String clusterName = "clean-after-close-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        runner.close();
        // Clean should work after close
        runner.clean();
        // Set runner to null to prevent double cleanup in tearDown
        runner = null;
    }

    // ==================== Multiple Close Calls ====================

    @Test
    public void testMultipleCloseCalls() throws Exception {
        final String clusterName = "multi-close-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        runner.close();
        // Second close should not throw
        runner.close();
        assertTrue(runner.isClosed());
    }
}

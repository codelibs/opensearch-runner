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
package org.codelibs.opensearch.runner.node;

import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;
import static org.junit.Assert.*;

import java.util.Collection;

import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.junit.After;
import org.junit.Test;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;

/**
 * Test cases for OpenSearchRunnerNode.
 */
public class OpenSearchRunnerNodeTest {

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

    @Test
    public void testNodeCreationWithDefaultModules() throws Exception {
        final String clusterName = "node-test-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final Node node = runner.node();
        assertNotNull(node);
        assertTrue(node instanceof OpenSearchRunnerNode);

        final OpenSearchRunnerNode runnerNode = (OpenSearchRunnerNode) node;
        assertNotNull(runnerNode.getPlugins());
    }

    @Test
    public void testGetPluginsReturnsNonEmptyCollection() throws Exception {
        final String clusterName = "plugins-test-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final Node node = runner.node();
        assertTrue(node instanceof OpenSearchRunnerNode);

        final OpenSearchRunnerNode runnerNode = (OpenSearchRunnerNode) node;
        final Collection<Class<? extends Plugin>> plugins = runnerNode.getPlugins();

        assertNotNull(plugins);
        // Default modules should be loaded
        assertFalse("Plugins collection should not be empty with default modules", plugins.isEmpty());
    }

    @Test
    public void testMultipleNodesHavePlugins() throws Exception {
        final String clusterName = "multi-node-plugins-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
            settingsBuilder.putList("cluster.initial_cluster_manager_nodes", "127.0.0.1:9301");
        }).build(newConfigs().clusterName(clusterName).numOfNode(2));
        runner.ensureYellow();

        for (int i = 0; i < runner.getNodeSize(); i++) {
            final Node node = runner.getNode(i);
            assertTrue(node instanceof OpenSearchRunnerNode);

            final OpenSearchRunnerNode runnerNode = (OpenSearchRunnerNode) node;
            assertNotNull("Node " + i + " should have plugins", runnerNode.getPlugins());
        }
    }

    @Test
    public void testNodePluginsAreConsistentAfterRestart() throws Exception {
        final String clusterName = "restart-plugins-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        // Get initial plugins
        OpenSearchRunnerNode initialNode = (OpenSearchRunnerNode) runner.node();
        final Collection<Class<? extends Plugin>> initialPlugins = initialNode.getPlugins();
        final int initialPluginCount = initialPlugins.size();

        // Close and restart the node
        runner.getNode(0).close();
        assertTrue(runner.startNode(0));
        runner.ensureYellow();

        // Verify plugins are still available
        OpenSearchRunnerNode restartedNode = (OpenSearchRunnerNode) runner.node();
        final Collection<Class<? extends Plugin>> restartedPlugins = restartedNode.getPlugins();

        assertNotNull(restartedPlugins);
        assertEquals("Plugin count should be the same after restart",
                initialPluginCount, restartedPlugins.size());
    }

    @Test
    public void testNodeIsNotClosedAfterCreation() throws Exception {
        final String clusterName = "not-closed-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final Node node = runner.node();
        assertFalse("Newly created node should not be closed", node.isClosed());
    }

    @Test
    public void testNodeSettings() throws Exception {
        final String clusterName = "settings-test-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();

        final Node node = runner.node();
        assertNotNull(node.settings());
        assertEquals("Node 1", node.settings().get("node.name"));
        assertEquals(clusterName, node.settings().get("cluster.name"));
    }
}

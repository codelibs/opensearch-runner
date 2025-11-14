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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.codelibs.opensearch.runner.OpenSearchRunnerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;

/**
 * Test cases for OpenSearchRunnerNode.
 */
public class OpenSearchRunnerNodeTest {

    private Path tempDir;
    private List<OpenSearchRunnerNode> nodesToCleanup;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("opensearch-runner-node-test");
        nodesToCleanup = new ArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
        // Close all nodes
        for (final OpenSearchRunnerNode node : nodesToCleanup) {
            if (node != null && !node.isClosed()) {
                try {
                    node.close();
                } catch (final Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
        nodesToCleanup.clear();

        // Delete temp directory
        if (tempDir != null && Files.exists(tempDir)) {
            try {
                deleteRecursively(tempDir);
            } catch (final Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    public void testConstructorWithEmptyPlugins() throws Exception {
        final Environment environment = createTestEnvironment("test-node-empty-plugins");
        final Collection<Class<? extends Plugin>> emptyPlugins = Collections.emptyList();

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, emptyPlugins);
        nodesToCleanup.add(node);

        assertNotNull(node);
        assertNotNull(node.getPlugins());
        assertTrue(node.getPlugins().isEmpty());
        assertTrue(node instanceof Node);
    }

    @Test
    public void testConstructorWithSinglePlugin() throws Exception {
        final Environment environment = createTestEnvironment("test-node-single-plugin");
        final Collection<Class<? extends Plugin>> plugins =
                Collections.singletonList(TestPlugin1.class);

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, plugins);
        nodesToCleanup.add(node);

        assertNotNull(node);
        assertNotNull(node.getPlugins());
        assertEquals(1, node.getPlugins().size());
        assertTrue(node.getPlugins().contains(TestPlugin1.class));
    }

    @Test
    public void testConstructorWithMultiplePlugins() throws Exception {
        final Environment environment = createTestEnvironment("test-node-multi-plugins");
        final Collection<Class<? extends Plugin>> plugins =
                Arrays.asList(TestPlugin1.class, TestPlugin2.class);

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, plugins);
        nodesToCleanup.add(node);

        assertNotNull(node);
        assertNotNull(node.getPlugins());
        assertEquals(2, node.getPlugins().size());
        assertTrue(node.getPlugins().contains(TestPlugin1.class));
        assertTrue(node.getPlugins().contains(TestPlugin2.class));
    }

    @Test
    public void testGetPlugins() throws Exception {
        final Environment environment = createTestEnvironment("test-node-get-plugins");
        final Collection<Class<? extends Plugin>> plugins =
                Arrays.asList(TestPlugin1.class, TestPlugin2.class);

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, plugins);
        nodesToCleanup.add(node);

        final Collection<Class<? extends Plugin>> retrievedPlugins = node.getPlugins();
        assertNotNull(retrievedPlugins);
        assertEquals(2, retrievedPlugins.size());
        assertTrue(retrievedPlugins.contains(TestPlugin1.class));
        assertTrue(retrievedPlugins.contains(TestPlugin2.class));
    }

    @Test
    public void testGetPluginsReturnsSameCollection() throws Exception {
        final Environment environment = createTestEnvironment("test-node-same-plugins");
        final Collection<Class<? extends Plugin>> plugins =
                Collections.singletonList(TestPlugin1.class);

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, plugins);
        nodesToCleanup.add(node);

        final Collection<Class<? extends Plugin>> retrieved1 = node.getPlugins();
        final Collection<Class<? extends Plugin>> retrieved2 = node.getPlugins();

        assertEquals(retrieved1, retrieved2);
    }

    @Test
    public void testNodeExtendsOpenSearchNode() throws Exception {
        final Environment environment = createTestEnvironment("test-node-extends");
        final Collection<Class<? extends Plugin>> emptyPlugins = Collections.emptyList();

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, emptyPlugins);
        nodesToCleanup.add(node);

        assertTrue(node instanceof Node);
    }

    @Test
    public void testNodeInitialization() throws Exception {
        final Environment environment = createTestEnvironment("test-node-init");
        final Collection<Class<? extends Plugin>> plugins =
                Collections.singletonList(TestPlugin1.class);

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, plugins);
        nodesToCleanup.add(node);

        assertNotNull(node);
        assertFalse(node.isClosed());
    }

    @Test
    public void testNodeCanBeClosed() throws Exception {
        final Environment environment = createTestEnvironment("test-node-close");
        final Collection<Class<? extends Plugin>> emptyPlugins = Collections.emptyList();

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, emptyPlugins);

        assertFalse(node.isClosed());
        node.close();
        assertTrue(node.isClosed());
    }

    @Test
    public void testMultipleNodesCanBeCreated() throws Exception {
        final Environment env1 = createTestEnvironment("test-node-multi-1");
        final Environment env2 = createTestEnvironment("test-node-multi-2");

        final OpenSearchRunnerNode node1 = new OpenSearchRunnerNode(
                env1, Collections.singletonList(TestPlugin1.class));
        final OpenSearchRunnerNode node2 = new OpenSearchRunnerNode(
                env2, Collections.singletonList(TestPlugin2.class));

        nodesToCleanup.add(node1);
        nodesToCleanup.add(node2);

        assertNotNull(node1);
        assertNotNull(node2);
        assertNotEquals(node1, node2);

        assertEquals(1, node1.getPlugins().size());
        assertEquals(1, node2.getPlugins().size());
        assertTrue(node1.getPlugins().contains(TestPlugin1.class));
        assertTrue(node2.getPlugins().contains(TestPlugin2.class));
    }

    @Test
    public void testNodeWithComplexPluginConfiguration() throws Exception {
        final Environment environment = createTestEnvironment("test-node-complex");
        final List<Class<? extends Plugin>> pluginList = new ArrayList<>();
        pluginList.add(TestPlugin1.class);
        pluginList.add(TestPlugin2.class);
        pluginList.add(TestPlugin3.class);

        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(environment, pluginList);
        nodesToCleanup.add(node);

        assertNotNull(node);
        assertEquals(3, node.getPlugins().size());
    }

    /**
     * Helper method to create a test Environment.
     */
    private Environment createTestEnvironment(final String nodeName) throws IOException {
        final Path nodeDir = tempDir.resolve(nodeName);
        Files.createDirectories(nodeDir);

        final Settings settings = Settings.builder()
                .put("node.name", nodeName)
                .put("path.home", nodeDir.toString())
                .put("http.port", "0")  // Random port
                .put("transport.type", "netty4")
                .put("transport.tcp.port", "0")  // Random port
                .putList("discovery.seed_hosts", Collections.emptyList())
                .putList("cluster.initial_cluster_manager_nodes", nodeName)
                .build();

        return InternalSettingsPreparer.prepareEnvironment(
                settings,
                Collections.emptyMap(),
                null,
                () -> nodeName);
    }

    /**
     * Helper method to delete a directory recursively.
     */
    private void deleteRecursively(final Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                stream.forEach(child -> {
                    try {
                        deleteRecursively(child);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
            }
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            // Ignore
        }
    }

    /**
     * Test plugin 1 for testing purposes.
     */
    public static class TestPlugin1 extends Plugin {
        public TestPlugin1() {
            super();
        }
    }

    /**
     * Test plugin 2 for testing purposes.
     */
    public static class TestPlugin2 extends Plugin {
        public TestPlugin2() {
            super();
        }
    }

    /**
     * Test plugin 3 for testing purposes.
     */
    public static class TestPlugin3 extends Plugin {
        public TestPlugin3() {
            super();
        }
    }
}

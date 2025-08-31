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

import static org.opensearch.common.settings.Settings.builder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.opensearch.runner.node.OpenSearchRunnerNode;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.flush.FlushRequestBuilder;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.opensearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.opensearch.action.delete.DeleteRequestBuilder;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.node.Node;
import org.opensearch.node.NodeValidationException;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;

import com.fasterxml.jackson.dataformat.smile.SmileConstants;

/**
 * OpenSearchRunner manages multiple OpenSearch instances.
 *
 * @author shinsuke
 *
 */
public class OpenSearchRunner implements Closeable {

    private static final Logger logger = LogManager
            .getLogger("opensearch.runner");

    private static final String NODE_NAME = "node.name";

    /** Configuration file name for Log4j2 properties. */
    protected static final String LOG4J2_PROPERTIES = "log4j2.properties";

    /** Configuration file name for OpenSearch YAML settings. */
    protected static final String ELASTICSEARCH_YAML = "opensearch.yml";

    /** Default module types to load in the OpenSearch cluster. */
    protected static final String[] MODULE_TYPES = { //
            "org.opensearch.search.aggregations.matrix.MatrixAggregationModulePlugin", //
            "org.opensearch.analysis.common.CommonAnalysisModulePlugin", //
            "org.opensearch.cache.common.tier.TieredSpilloverCachePlugin", //
            "org.opensearch.geo.GeoModulePlugin", //
            "org.opensearch.ingest.common.IngestCommonModulePlugin", //
            // "org.opensearch.ingest.geoip.IngestGeoIpModulePlugin", //
            "org.opensearch.ingest.useragent.IngestUserAgentModulePlugin", //
            "org.opensearch.script.expression.ExpressionModulePlugin", //
            "org.opensearch.script.mustache.MustacheModulePlugin", //
            "org.opensearch.painless.PainlessModulePlugin", //
            "org.opensearch.index.mapper.MapperExtrasModulePlugin", //
            "org.opensearch.dashboards.OpenSearchDashboardsModulePlugin", //
            "org.opensearch.join.ParentJoinModulePlugin", //
            "org.opensearch.percolator.PercolatorModulePlugin", //
            "org.opensearch.index.rankeval.RankEvalModulePlugin", //
            "org.opensearch.index.reindex.ReindexModulePlugin", //
            "org.opensearch.plugin.repository.url.URLRepositoryModulePlugin", //
            "org.opensearch.search.pipeline.common.SearchPipelineCommonModulePlugin", //
            "org.opensearch.systemd.SystemdModulePlugin", //
            "org.opensearch.transport.Netty4ModulePlugin" //
    };

    /** Default directory name for data storage. */
    protected static final String DATA_DIR = "data";

    /** Default directory name for log storage. */
    protected static final String LOGS_DIR = "logs";

    /** Default directory name for configuration files. */
    protected static final String CONFIG_DIR = "config";

    /** List of OpenSearch nodes in the cluster. */
    protected List<OpenSearchRunnerNode> nodeList = new ArrayList<>();

    /** List of environment configurations for each node. */
    protected List<Environment> envList = new ArrayList<>();

    /** Collection of module classes to load. */
    protected Collection<Class<? extends Plugin>> moduleList = new ArrayList<>();

    /** Collection of plugin classes to load. */
    protected Collection<Class<? extends Plugin>> pluginList = new ArrayList<>();

    /** Maximum HTTP port number for node discovery. */
    protected int maxHttpPort = 9299;

    /** Base path for OpenSearch data and configuration files. */
    @Option(name = "-basePath", usage = "Base path for OpenSearch.")
    protected String basePath;

    /** Path to configuration files directory. */
    @Option(name = "-confPath", usage = "Config path for OpenSearch.")
    protected String confPath;

    /** Path to data storage directory. */
    @Option(name = "-dataPath", usage = "Data path for OpenSearch.")
    protected String dataPath;

    /** Path to logs directory. */
    @Option(name = "-logsPath", usage = "Log path for OpenSearch.")
    protected String logsPath;

    /** Number of nodes to create in the cluster. */
    @Option(name = "-numOfNode", usage = "The number of OpenSearch node.")
    protected int numOfNode = 3;

    /** Base HTTP port number for the first node. */
    @Option(name = "-baseHttpPort", usage = "Base http port.")
    protected int baseHttpPort = 9200;

    /** Name of the OpenSearch cluster. */
    @Option(name = "-clusterName", usage = "Cluster name.")
    protected String clusterName = "opensearch-runner";

    /** Type of index store to use (e.g., 'fs' for filesystem). */
    @Option(name = "-indexStoreType", usage = "Index store type.")
    protected String indexStoreType = "fs";

    /** Whether to use logger for output instead of System.out. */
    @Option(name = "-useLogger", usage = "Print logs to a logger.")
    protected boolean useLogger = false;

    /** Whether to disable OpenSearch internal logging. */
    @Option(name = "-disableESLogger", usage = "Disable ESLogger.")
    protected boolean disableESLogger = false;

    /** Whether to print stack traces on operation failures. */
    @Option(name = "-printOnFailure", usage = "Print an exception on a failure.")
    protected boolean printOnFailure = false;

    /** Comma-separated list of module types to load. */
    @Option(name = "-moduleTypes", usage = "Module types.")
    protected String moduleTypes;

    /** Comma-separated list of plugin types to load. */
    @Option(name = "-pluginTypes", usage = "Plugin types.")
    protected String pluginTypes;

    /** Custom settings builder for node configuration. */
    protected Builder settingsBuilder;

    /**
     * Main method to run OpenSearch cluster from command line.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args) {
        try (final OpenSearchRunner runner = new OpenSearchRunner()) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        runner.close();
                    } catch (final IOException e) {
                        runner.print(e.getLocalizedMessage());
                    }
                }
            });

            runner.build(args);

            while (true) {
                if (runner.isClosed()) {
                    break;
                }
                Thread.sleep(5000);
            }
        } catch (final Exception e) {
            System.exit(1);
        }
    }

    /**
     * Constructs a new OpenSearchRunner instance with default settings.
     */
    public OpenSearchRunner() {
        // nothing
    }

    /**
     * Check if a cluster runner is closed.
     *
     * @return true if a runner is closed.
     */
    public boolean isClosed() {
        for (final Node node : nodeList) {
            if (!node.isClosed()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Close a cluster runner.
     * @throws IOException i/o exception
     */
    @Override
    public void close() throws IOException {
        final List<IOException> exceptionList = new ArrayList<>();
        for (final Node node : nodeList) {
            try {
                node.close();
                if (!node.awaitClose(10, TimeUnit.SECONDS)) {
                    print("Failed to close node: "
                            + node.settings().get(NODE_NAME, "unknown"));
                }
            } catch (final InterruptedException e) {
                logger.debug("Interupted closing process.", e);
            } catch (final IOException e) {
                exceptionList.add(e);
            }
        }
        if (!exceptionList.isEmpty()) {
            if (useLogger && logger.isDebugEnabled()) {
                for (final Exception e : exceptionList) {
                    logger.debug("Failed to close a node.", e);
                }
            }
            throw new IOException(exceptionList.toString());
        }
        print("Closed all nodes.");
    }

    /**
     * Delete all configuration files and directories.
     */
    public void clean() {
        LogManager.shutdown();
        final Path bPath = FileSystems.getDefault().getPath(basePath);
        final CleanUpFileVisitor visitor = new CleanUpFileVisitor();
        try {
            Files.walkFileTree(bPath, visitor);
            if (visitor.hasErrors()) {
                throw new OpenSearchRunnerException(visitor.getErrors().stream()
                        .map(Throwable::getLocalizedMessage)
                        .collect(Collectors.joining("\n")));
            }
        } catch (final IOException e) {
            throw new OpenSearchRunnerException("Failed to delete " + bPath, e);
        }
    }

    /**
     * Configure each OpenSearch instance by builder.
     *
     * @param builder builder to create a cluster
     * @return this instance
     */
    public OpenSearchRunner onBuild(final Builder builder) {
        this.settingsBuilder = builder;
        return this;
    }

    /**
     * Create and start OpenSearch cluster with Configs instance.
     *
     * @param configs configuration
     */
    public void build(final Configs configs) {
        build(configs.build());
    }

    /**
     * Create and start OpenSearch cluster with arguments.
     *
     * @param args artuments for starting a cluster
     */
    public void build(final String... args) {
        if (args != null) {
            final CmdLineParser parser = new CmdLineParser(this,
                    ParserProperties.defaults().withUsageWidth(80));

            try {
                parser.parseArgument(args);
            } catch (final CmdLineException e) {
                throw new OpenSearchRunnerException(
                        "Failed to parse args: " + args != null
                                ? String.join(" ", args)
                                : "");
            }
        }

        if (basePath == null) {
            try {
                basePath = Files.createTempDirectory("opensearch-cluster")
                        .toAbsolutePath().toString();
            } catch (final IOException e) {
                throw new OpenSearchRunnerException(
                        "Could not create $ES_HOME.", e);
            }
        }

        final Path esBasePath = Paths.get(basePath);
        createDir(esBasePath);

        final String[] types = moduleTypes == null ? MODULE_TYPES
                : moduleTypes.split(",");
        for (final String moduleType : types) {
            Class<? extends Plugin> clazz;
            try {
                clazz = Class.forName(moduleType).asSubclass(Plugin.class);
                moduleList.add(clazz);
            } catch (final ClassNotFoundException e) {
                logger.debug("{} is not found.", moduleType, e);
            }
        }
        if (pluginTypes != null) {
            for (final String value : pluginTypes.split(",")) {
                final String pluginType = value.trim();
                if (pluginType.length() > 0) {
                    Class<? extends Plugin> clazz;
                    try {
                        clazz = Class.forName(pluginType)
                                .asSubclass(Plugin.class);
                        pluginList.add(clazz);
                    } catch (final ClassNotFoundException e) {
                        throw new OpenSearchRunnerException(
                                pluginType + " is not found.", e);
                    }
                }
            }
        }

        print("Cluster Name: " + clusterName);
        print("Base Path:    " + basePath);
        print("Num Of Node:  " + numOfNode);

        for (int i = 0; i < numOfNode; i++) {
            execute(i + 1);
        }
    }

    /**
     * Executes the setup and start process for a node with the specified ID.
     *
     * @param id the node ID
     */
    protected void execute(final int id) {
        final Path homePath = Paths.get(basePath, "node_" + id);
        final Path confPath = this.confPath == null
                ? homePath.resolve(CONFIG_DIR)
                : Paths.get(this.confPath);
        final Path logsPath = this.logsPath == null ? homePath.resolve(LOGS_DIR)
                : Paths.get(this.logsPath);
        final Path dataPath = this.dataPath == null ? homePath.resolve(DATA_DIR)
                : Paths.get(this.dataPath);

        createDir(homePath);
        createDir(confPath);
        createDir(logsPath);
        createDir(dataPath);

        final Settings.Builder builder = builder();

        if (settingsBuilder != null) {
            settingsBuilder.build(id, builder);
        }

        putIfAbsent(builder, "path.home", homePath.toAbsolutePath().toString());
        putIfAbsent(builder, "path.data", dataPath.toAbsolutePath().toString());
        putIfAbsent(builder, "path.logs", logsPath.toAbsolutePath().toString());

        final Path esConfPath = confPath.resolve(ELASTICSEARCH_YAML);
        if (!esConfPath.toFile().exists()) {
            try (InputStream is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(
                            CONFIG_DIR + "/" + ELASTICSEARCH_YAML)) {
                Files.copy(is, esConfPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new OpenSearchRunnerException(
                        "Could not create: " + esConfPath, e);
            }
        }

        if (!disableESLogger) {
            final Path logConfPath = confPath.resolve(LOG4J2_PROPERTIES);
            if (!logConfPath.toFile().exists()) {
                try (InputStream is = Thread.currentThread()
                        .getContextClassLoader().getResourceAsStream(
                                CONFIG_DIR + "/" + LOG4J2_PROPERTIES)) {
                    Files.copy(is, logConfPath,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (final IOException e) {
                    throw new OpenSearchRunnerException(
                            "Could not create: " + logConfPath, e);
                }
            }
        }

        final Collection<Class<? extends Plugin>> moduleAndPluginList = new ArrayList<>();
        try {
            final String modulePath = builder.get("path.modules");
            if (modulePath != null) {
                final Path sourcePath = Paths.get(modulePath);
                final Path targetPath = homePath.resolve("modules");
                Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(final Path dir,
                            final BasicFileAttributes attrs)
                            throws IOException {
                        Files.createDirectories(
                                targetPath.resolve(sourcePath.relativize(dir)));
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(final Path file,
                            final BasicFileAttributes attrs)
                            throws IOException {
                        Files.copy(file,
                                targetPath.resolve(sourcePath.relativize(file)),
                                StandardCopyOption.REPLACE_EXISTING);
                        return FileVisitResult.CONTINUE;
                    }
                });
                builder.remove("path.modules");
            } else {
                moduleAndPluginList.addAll(moduleList);
            }
            moduleAndPluginList.addAll(pluginList);

            final String pluginPath = builder.get("path.plugins");
            if (pluginPath != null) {
                final Path sourcePath = Paths.get(pluginPath);
                final Path targetPath = homePath.resolve("plugins");
                Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(final Path dir,
                            final BasicFileAttributes attrs)
                            throws IOException {
                        Files.createDirectories(
                                targetPath.resolve(sourcePath.relativize(dir)));
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(final Path file,
                            final BasicFileAttributes attrs)
                            throws IOException {
                        Files.copy(file,
                                targetPath.resolve(sourcePath.relativize(file)),
                                StandardCopyOption.REPLACE_EXISTING);
                        return FileVisitResult.CONTINUE;
                    }
                });
                builder.remove("path.plugins");
            }

            final String nodeName = "Node " + id;
            final int httpPort = getAvailableHttpPort(id);
            putIfAbsent(builder, "cluster.name", clusterName);
            putIfAbsent(builder, NODE_NAME, nodeName);
            putIfAbsent(builder, "http.port", String.valueOf(httpPort));
            putIfAbsent(builder, "index.store.type", indexStoreType);
            if (!builder.keys().contains("node.roles")) {
                builder.putList("node.roles", "cluster_manager", "data");
            }

            print("Node Name:      " + nodeName);
            print("HTTP Port:      " + httpPort);
            print("Data Directory: " + dataPath);
            print("Log Directory:  " + logsPath);

            final Settings settings = builder.build();
            final Environment environment = InternalSettingsPreparer
                    .prepareEnvironment(settings, Collections.emptyMap(),
                            confPath, () -> nodeName);
            if (!disableESLogger) {
                LogConfigurator.registerErrorListener();
                // LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(environment.settings()));
                LogConfigurator.configure(environment);
            }
            createDir(environment.modulesDir());
            createDir(environment.pluginsDir());

            final OpenSearchRunnerNode node = new OpenSearchRunnerNode(
                    environment, moduleAndPluginList);
            node.start();
            nodeList.add(node);
            envList.add(environment);
        } catch (final Exception e) {
            throw new OpenSearchRunnerException("Failed to start node " + id,
                    e);
        }
    }

    /**
     * Gets an available HTTP port starting from the base port plus the node number.
     *
     * @param number the node number
     * @return an available HTTP port number
     */
    protected int getAvailableHttpPort(final int number) {
        int httpPort = baseHttpPort + number;
        if (maxHttpPort < 0) {
            return httpPort;
        }
        while (httpPort <= maxHttpPort) {
            try (Socket socket = new Socket("localhost", httpPort)) {
                httpPort++;
            } catch (final ConnectException e) {
                return httpPort;
            } catch (final IOException e) {
                print(e.getMessage());
                httpPort++;
            }
        }
        throw new OpenSearchRunnerException(
                "The http port " + httpPort + " is unavailable.");
    }

    /**
     * Adds a setting to the builder if it's not already present.
     *
     * @param builder the settings builder
     * @param key the setting key
     * @param value the setting value
     */
    protected void putIfAbsent(final Settings.Builder builder, final String key,
            final String value) {
        if (builder.get(key) == null && value != null) {
            builder.put(key, value);
        }
    }

    /**
     * Sets the maximum HTTP port for node discovery.
     *
     * @param maxHttpPort the maximum HTTP port number
     */
    public void setMaxHttpPort(final int maxHttpPort) {
        this.maxHttpPort = maxHttpPort;
    }

    /**
     * Return a node by the node index.
     *
     * @param i A node index
     * @return null if the node is not found
     */
    public Node getNode(final int i) {
        if (i < 0 || i >= nodeList.size()) {
            return null;
        }
        return nodeList.get(i);
    }

    /**
     * Start a closed node.
     *
     * @param i the number of nodes
     * @return true if the node is started.
     */
    public boolean startNode(final int i) {
        if ((i >= nodeList.size()) || !nodeList.get(i).isClosed()) {
            return false;
        }
        final OpenSearchRunnerNode node = new OpenSearchRunnerNode(
                envList.get(i), nodeList.get(i).getPlugins());
        try {
            node.start();
            nodeList.set(i, node);
            return true;
        } catch (final NodeValidationException e) {
            print(e.getLocalizedMessage());
        }
        return false;
    }

    /**
     * Return a node by the name.
     *
     * @param name A node name
     * @return null if the node is not found by the name
     */
    public Node getNode(final String name) {
        if (name == null) {
            return null;
        }
        for (final Node node : nodeList) {
            if (name.equals(node.settings().get(NODE_NAME))) {
                return node;
            }
        }
        return null;
    }

    /**
     * Return a node index.
     *
     * @param node node to check an index
     * @return -1 if the node does not exist.
     */
    public int getNodeIndex(final Node node) {
        for (int i = 0; i < nodeList.size(); i++) {
            if (nodeList.get(i).equals(node)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Return the number of nodes.
     *
     * @return the number of nodes
     */
    public int getNodeSize() {
        return nodeList.size();
    }

    /**
     * Prints a message using the configured output method.
     *
     * @param line the message to print
     */
    public void print(final String line) {
        if (useLogger) {
            logger.info(line);
        } else {
            System.out.println(line);
        }
    }

    /**
     * Creates a directory at the specified path if it doesn't exist.
     *
     * @param path the directory path to create
     */
    protected void createDir(final Path path) {
        if (!path.toFile().exists()) {
            print("Creating " + path);
            try {
                Files.createDirectories(path);
            } catch (final IOException e) {
                throw new OpenSearchRunnerException("Failed to create " + path,
                        e);
            }
        }
    }

    /**
     * Return an available node.
     *
     * @return node
     */
    public Node node() {
        for (final Node node : nodeList) {
            if (!node.isClosed()) {
                return node;
            }
        }
        throw new OpenSearchRunnerException("All nodes are closed.");
    }

    /**
     * Return a cluster manager node.
     *
     * @return master node
     */
    public synchronized Node clusterManagerNode() {
        final ClusterState state = client().admin().cluster().prepareState()
                .execute().actionGet().getState();
        final String name = state.nodes().getClusterManagerNode().getName();
        return getNode(name);
    }

    /**
     * Return a non-master node.
     *
     * @return non-master node
     */
    public synchronized Node nonClusterManagerNode() {
        final ClusterState state = client().admin().cluster().prepareState()
                .execute().actionGet().getState();
        final String name = state.nodes().getClusterManagerNode().getName();
        for (final Node node : nodeList) {
            if (!node.isClosed()
                    && !name.equals(node.settings().get(NODE_NAME))) {
                return node;
            }
        }
        return null;
    }

    /**
     * Return an opensearch client.
     *
     * @return client
     */
    public Client client() {
        return node().client();
    }

    /**
     * Return an opensearch admin client.
     *
     * @return admin client
     */
    public AdminClient admin() {
        return client().admin();
    }

    /**
     * Wait for green state of a cluster.
     *
     * @param indices indices to check status
     * @return cluster health status
     */
    public ClusterHealthStatus ensureGreen(final String... indices) {
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices)
                        .waitForGreenStatus().waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true))
                .actionGet();
        if (actionGet.isTimedOut()) {
            onFailure(
                    "ensureGreen timed out, cluster state:\n" + client()
                            .admin().cluster().prepareState().get().getState()
                            + "\n"
                            + client().admin().cluster()
                                    .preparePendingClusterTasks().get(),
                    actionGet);
        }
        return actionGet.getStatus();
    }

    /**
     * Wait for yellow state of a cluster.
     *
     * @param indices indices to check status
     * @return cluster health status
     */
    public ClusterHealthStatus ensureYellow(final String... indices) {
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices)
                        .waitForNoRelocatingShards(true).waitForYellowStatus()
                        .waitForEvents(Priority.LANGUID))
                .actionGet();
        if (actionGet.isTimedOut()) {
            onFailure(
                    "ensureYellow timed out, cluster state:\n" + "\n" + client()
                            .admin().cluster().prepareState().get().getState()
                            + "\n"
                            + client().admin().cluster()
                                    .preparePendingClusterTasks().get(),
                    actionGet);
        }
        return actionGet.getStatus();
    }

    /**
     * Waits for shard relocation to complete.
     *
     * @return the cluster health status after relocation
     */
    public ClusterHealthStatus waitForRelocation() {
        final ClusterHealthRequest request = Requests.clusterHealthRequest()
                .waitForNoRelocatingShards(true);
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .health(request).actionGet();
        if (actionGet.isTimedOut()) {
            onFailure(
                    "waitForRelocation timed out, cluster state:\n" + "\n"
                            + client().admin().cluster().prepareState().get()
                                    .getState()
                            + "\n"
                            + client().admin().cluster()
                                    .preparePendingClusterTasks().get(),
                    actionGet);
        }
        return actionGet.getStatus();
    }

    /**
     * Flushes all indices in the cluster.
     *
     * @return flush response
     */
    public FlushResponse flush() {
        return flush(true);
    }

    /**
     * Flushes all indices in the cluster with force option.
     *
     * @param force whether to force the flush
     * @return flush response
     */
    public FlushResponse flush(final boolean force) {
        return flush(builder -> builder.setWaitIfOngoing(true).setForce(force));
    }

    /**
     * Flushes indices with custom flush configuration.
     *
     * @param builder callback to configure the flush request
     * @return flush response
     */
    public FlushResponse flush(
            final BuilderCallback<FlushRequestBuilder> builder) {
        waitForRelocation();
        final FlushResponse actionGet = builder
                .apply(client().admin().indices().prepareFlush()).execute()
                .actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    /**
     * Refreshes all indices in the cluster.
     *
     * @return refresh response
     */
    public RefreshResponse refresh() {
        return refresh(builder -> builder);
    }

    /**
     * Refreshes indices with custom refresh configuration.
     *
     * @param builder callback to configure the refresh request
     * @return refresh response
     */
    public RefreshResponse refresh(
            final BuilderCallback<RefreshRequestBuilder> builder) {
        waitForRelocation();
        final RefreshResponse actionGet = builder
                .apply(client().admin().indices().prepareRefresh()).execute()
                .actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    /**
     * Upgrades all indices in the cluster.
     *
     * @return upgrade response
     */
    public UpgradeResponse upgrade() {
        return upgrade(true);
    }

    /**
     * Upgrades all indices with option to upgrade only ancient segments.
     *
     * @param upgradeOnlyAncientSegments whether to upgrade only ancient segments
     * @return upgrade response
     */
    public UpgradeResponse upgrade(final boolean upgradeOnlyAncientSegments) {
        return upgrade(builder -> builder
                .setUpgradeOnlyAncientSegments(upgradeOnlyAncientSegments));
    }

    /**
     * Upgrades indices with custom upgrade configuration.
     *
     * @param builder callback to configure the upgrade request
     * @return upgrade response
     */
    public UpgradeResponse upgrade(
            final BuilderCallback<UpgradeRequestBuilder> builder) {
        waitForRelocation();
        final UpgradeResponse actionGet = builder
                .apply(client().admin().indices().prepareUpgrade()).execute()
                .actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    /**
     * Force merges all indices in the cluster.
     *
     * @return force merge response
     */
    public ForceMergeResponse forceMerge() {
        return forceMerge(-1, false, true);
    }

    /**
     * Force merges indices with specified parameters.
     *
     * @param maxNumSegments maximum number of segments per shard
     * @param onlyExpungeDeletes whether to only expunge deleted documents
     * @param flush whether to flush after the merge
     * @return force merge response
     */
    public ForceMergeResponse forceMerge(final int maxNumSegments,
            final boolean onlyExpungeDeletes, final boolean flush) {
        return forceMerge(builder -> builder.setMaxNumSegments(maxNumSegments)
                .setOnlyExpungeDeletes(onlyExpungeDeletes).setFlush(flush));
    }

    /**
     * Force merges indices with custom configuration.
     *
     * @param builder callback to configure the force merge request
     * @return force merge response
     */
    public ForceMergeResponse forceMerge(
            final BuilderCallback<ForceMergeRequestBuilder> builder) {
        waitForRelocation();
        final ForceMergeResponse actionGet = builder
                .apply(client().admin().indices().prepareForceMerge()).execute()
                .actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    /**
     * Opens the specified index.
     *
     * @param index the index name to open
     * @return open index response
     */
    public OpenIndexResponse openIndex(final String index) {
        return openIndex(index, builder -> builder);
    }

    /**
     * Opens the specified index with custom request configuration.
     *
     * @param index the index name to open
     * @param builder callback to configure the open index request
     * @return open index response
     */
    public OpenIndexResponse openIndex(final String index,
            final BuilderCallback<OpenIndexRequestBuilder> builder) {
        final OpenIndexResponse actionGet = builder
                .apply(client().admin().indices().prepareOpen(index)).execute()
                .actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to open " + index + ".", actionGet);
        }
        return actionGet;
    }

    /**
     * Closes the specified index.
     *
     * @param index the index name to close
     * @return the close index response
     */
    public CloseIndexResponse closeIndex(final String index) {
        return closeIndex(index, builder -> builder);
    }

    /**
     * Closes the specified index with custom request configuration.
     *
     * @param index the index name to close
     * @param builder callback to configure the close index request
     * @return the close index response
     */
    public CloseIndexResponse closeIndex(final String index,
            final BuilderCallback<CloseIndexRequestBuilder> builder) {
        final CloseIndexResponse actionGet = builder
                .apply(client().admin().indices().prepareClose(index)).execute()
                .actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to close " + index + ".", actionGet);
        }
        return actionGet;
    }

    /**
     * Creates an index with the specified settings.
     *
     * @param index the index name to create
     * @param settings the index settings
     * @return create index response
     */
    public CreateIndexResponse createIndex(final String index,
            final Settings settings) {
        return createIndex(index, builder -> builder.setSettings(
                settings != null ? settings : Settings.Builder.EMPTY_SETTINGS));
    }

    /**
     * Creates an index with custom configuration.
     *
     * @param index the index name to create
     * @param builder callback to configure the create index request
     * @return create index response
     */
    public CreateIndexResponse createIndex(final String index,
            final BuilderCallback<CreateIndexRequestBuilder> builder) {
        final CreateIndexResponse actionGet = builder
                .apply(client().admin().indices().prepareCreate(index))
                .execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create " + index + ".", actionGet);
        }
        return actionGet;
    }

    /**
     * Checks if the specified index exists.
     *
     * @param index the index name to check
     * @return true if the index exists, false otherwise
     */
    public boolean indexExists(final String index) {
        return indexExists(index, builder -> builder);
    }

    /**
     * Checks if the specified index exists with custom configuration.
     *
     * @param index the index name to check
     * @param builder callback to configure the indices exists request
     * @return true if the index exists, false otherwise
     */
    public boolean indexExists(final String index,
            final BuilderCallback<IndicesExistsRequestBuilder> builder) {
        final IndicesExistsResponse actionGet = builder
                .apply(client().admin().indices().prepareExists(index))
                .execute().actionGet();
        return actionGet.isExists();
    }

    /**
     * Deletes the specified index.
     *
     * @param index the index name to delete
     * @return acknowledged response
     */
    public AcknowledgedResponse deleteIndex(final String index) {
        return deleteIndex(index, builder -> builder);
    }

    /**
     * Deletes the specified index with custom configuration.
     *
     * @param index the index name to delete
     * @param builder callback to configure the delete index request
     * @return acknowledged response
     */
    public AcknowledgedResponse deleteIndex(final String index,
            final BuilderCallback<DeleteIndexRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder
                .apply(client().admin().indices().prepareDelete(index))
                .execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create " + index + ".", actionGet);
        }
        return actionGet;
    }

    /**
     * Creates mapping for the specified index.
     *
     * @param index the index name
     * @param mappingSource the mapping definition as JSON string
     * @return acknowledged response
     */
    public AcknowledgedResponse createMapping(final String index,
            final String mappingSource) {
        return createMapping(index, builder -> builder.setSource(mappingSource,
                xContentType(mappingSource)));
    }

    /**
     * Creates mapping for the specified index using XContentBuilder.
     *
     * @param index the index name
     * @param source the mapping definition as XContentBuilder
     * @return acknowledged response
     */
    public AcknowledgedResponse createMapping(final String index,
            final XContentBuilder source) {
        return createMapping(index, builder -> builder.setSource(source));
    }

    /**
     * Creates mapping for the specified index with custom configuration.
     *
     * @param index the index name
     * @param builder callback to configure the put mapping request
     * @return acknowledged response
     */
    public AcknowledgedResponse createMapping(final String index,
            final BuilderCallback<PutMappingRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder
                .apply(client().admin().indices().preparePutMapping(index))
                .execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create a mapping for " + index + ".",
                    actionGet);
        }
        return actionGet;
    }

    /**
     * Inserts a document into the specified index.
     *
     * @param index the index name
     * @param id the document ID
     * @param source the document source
     * @return index response
     */
    public IndexResponse insert(final String index, final String id,
            final String source) {
        return insert(index, id,
                builder -> builder.setSource(source, xContentType(source))
                        .setRefreshPolicy(RefreshPolicy.IMMEDIATE));
    }

    /**
     * Inserts a document into the specified index with custom configuration.
     *
     * @param index the index name
     * @param id the document ID
     * @param builder callback to configure the index request
     * @return index response
     */
    public IndexResponse insert(final String index, final String id,
            final BuilderCallback<IndexRequestBuilder> builder) {
        final IndexResponse actionGet = builder
                .apply(client().prepareIndex().setIndex(index).setId(id))
                .execute().actionGet();
        if (actionGet.getResult() != Result.CREATED) {
            onFailure("Failed to insert " + id + " into " + index + ".",
                    actionGet);
        }
        return actionGet;
    }

    /**
     * Deletes a document from the specified index.
     *
     * @param index the index name
     * @param id the document ID
     * @return delete response
     */
    public DeleteResponse delete(final String index, final String id) {
        return delete(index, id,
                builder -> builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE));
    }

    /**
     * Deletes a document from the specified index with custom configuration.
     *
     * @param index the index name
     * @param id the document ID
     * @param builder callback to configure the delete request
     * @return delete response
     */
    public DeleteResponse delete(final String index, final String id,
            final BuilderCallback<DeleteRequestBuilder> builder) {
        final DeleteResponse actionGet = builder
                .apply(client().prepareDelete().setIndex(index).setId(id))
                .execute().actionGet();
        if (actionGet.getResult() != Result.DELETED) {
            onFailure("Failed to delete " + id + " from " + index + ".",
                    actionGet);
        }
        return actionGet;
    }

    /**
     * Counts documents in the specified index.
     *
     * @param index the index name
     * @return search response containing the count
     */
    public SearchResponse count(final String index) {
        return count(index, builder -> builder);
    }

    /**
     * Counts documents in the specified index with custom search configuration.
     *
     * @param index the index name
     * @param builder callback to configure the search request
     * @return search response containing the count
     */
    public SearchResponse count(final String index,
            final BuilderCallback<SearchRequestBuilder> builder) {
        return builder.apply(client().prepareSearch(index).setSize(0)).execute()
                .actionGet();
    }

    /**
     * Searches documents in the specified index with query, sort, and pagination parameters.
     *
     * @param index the index name
     * @param queryBuilder the query to execute
     * @param sort the sort configuration
     * @param from the starting position for results
     * @param size the number of results to return
     * @return search response containing the results
     */
    public SearchResponse search(final String index,
            final QueryBuilder queryBuilder, final SortBuilder<?> sort,
            final int from, final int size) {
        return search(index,
                builder -> builder
                        .setQuery(queryBuilder != null ? queryBuilder
                                : QueryBuilders.matchAllQuery())
                        .addSort(sort != null ? sort : SortBuilders.scoreSort())
                        .setFrom(from).setSize(size));
    }

    /**
     * Searches documents in the specified index with custom search configuration.
     *
     * @param index the index name
     * @param builder callback to configure the search request
     * @return search response containing the results
     */
    public SearchResponse search(final String index,
            final BuilderCallback<SearchRequestBuilder> builder) {
        return builder.apply(client().prepareSearch(index)).execute()
                .actionGet();
    }

    /**
     * Gets information about the specified alias.
     *
     * @param alias the alias name
     * @return get aliases response
     */
    public GetAliasesResponse getAlias(final String alias) {
        return getAlias(alias, builder -> builder);
    }

    /**
     * Gets information about the specified alias with custom configuration.
     *
     * @param alias the alias name
     * @param builder callback to configure the get aliases request
     * @return get aliases response
     */
    public GetAliasesResponse getAlias(final String alias,
            final BuilderCallback<GetAliasesRequestBuilder> builder) {
        return builder
                .apply(client().admin().indices().prepareGetAliases(alias))
                .execute().actionGet();
    }

    /**
     * Updates an alias by adding and removing indices.
     *
     * @param alias the alias name
     * @param addedIndices indices to add to the alias
     * @param deletedIndices indices to remove from the alias
     * @return acknowledged response
     */
    public AcknowledgedResponse updateAlias(final String alias,
            final String[] addedIndices, final String[] deletedIndices) {
        return updateAlias(builder -> {
            if (addedIndices != null && addedIndices.length > 0) {
                builder.addAlias(addedIndices, alias);
            }
            if (deletedIndices != null && deletedIndices.length > 0) {
                builder.removeAlias(deletedIndices, alias);
            }
            return builder;
        });
    }

    /**
     * Updates aliases with custom configuration.
     *
     * @param builder callback to configure the indices aliases request
     * @return acknowledged response
     */
    public AcknowledgedResponse updateAlias(
            final BuilderCallback<IndicesAliasesRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder
                .apply(client().admin().indices().prepareAliases()).execute()
                .actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to update aliases.", actionGet);
        }
        return actionGet;
    }

    /**
     * Gets the cluster service instance.
     *
     * @return the ClusterService instance
     */
    public ClusterService clusterService() {
        return getInstance(ClusterService.class);
    }

    /**
     * Gets an instance of the specified service class from the first available node.
     *
     * @param <T> the service type
     * @param clazz the service class
     * @return the service instance
     */
    public synchronized <T> T getInstance(final Class<T> clazz) {
        final Node node = clusterManagerNode();
        return node.injector().getInstance(clazz);
    }

    /**
     * Gets the name of the cluster.
     *
     * @return the cluster name
     */
    public String getClusterName() {
        return clusterName;
    }

    private void onFailure(final String message,
            final ActionResponse response) {
        if (!printOnFailure) {
            throw new OpenSearchRunnerException(message, response);
        }
        print(message);
    }

    private static final class CleanUpFileVisitor implements FileVisitor<Path> {
        private final List<Throwable> errorList = new ArrayList<>();

        @Override
        public FileVisitResult preVisitDirectory(final Path dir,
                final BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        public boolean hasErrors() {
            return !errorList.isEmpty();
        }

        public List<Throwable> getErrors() {
            return errorList;
        }

        @Override
        public FileVisitResult visitFile(final Path file,
                final BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return checkIfExist(file);
        }

        @Override
        public FileVisitResult visitFileFailed(final Path file,
                final IOException exc) throws IOException {
            throw exc;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir,
                final IOException exc) throws IOException {
            if (exc != null) {
                throw exc;
            }
            Files.delete(dir);
            if (dir.toFile().exists()) {
                errorList.add(new IOException("Failed to delete " + dir));
                dir.toFile().deleteOnExit();
            }
            return FileVisitResult.CONTINUE;
        }

        private FileVisitResult checkIfExist(final Path path) {
            if (path.toFile().exists()) {
                errorList.add(new IOException("Failed to delete " + path));
                path.toFile().deleteOnExit();
            }
            return FileVisitResult.CONTINUE;
        }
    }

    /**
     * This builder sets parameters to create a node
     *
     */
    public interface Builder {

        /**
         * Configures settings for a specific node.
         *
         * @param index an index of nodes
         * @param builder a builder instance to create a node
         */
        void build(int index, Settings.Builder builder);
    }

    /**
     * Creates a new configuration builder for OpenSearchRunner.
     *
     * @return a new Configs instance
     */
    public static Configs newConfigs() {
        return new Configs();
    }

    /**
     * Configuration builder for OpenSearchRunner command line arguments.
     */
    public static class Configs {
        List<String> configList = new ArrayList<>();

        /**
         * Creates a new Configs instance for building OpenSearchRunner command line arguments.
         */
        public Configs() {
            // Default constructor
        }

        /**
         * Sets the base path for OpenSearch data and configuration files.
         *
         * @param basePath the base path
         * @return this Configs instance for method chaining
         */
        public Configs basePath(final String basePath) {
            configList.add("-basePath");
            configList.add(basePath);
            return this;
        }

        /**
         * Sets the number of nodes in the cluster.
         *
         * @param numOfNode the number of nodes
         * @return this Configs instance for method chaining
         */
        public Configs numOfNode(final int numOfNode) {
            configList.add("-numOfNode");
            configList.add(String.valueOf(numOfNode));
            return this;
        }

        /**
         * Sets the base HTTP port for the first node.
         *
         * @param baseHttpPort the base HTTP port
         * @return this Configs instance for method chaining
         */
        public Configs baseHttpPort(final int baseHttpPort) {
            configList.add("-baseHttpPort");
            configList.add(String.valueOf(baseHttpPort));
            return this;
        }

        /**
         * Sets the cluster name.
         *
         * @param clusterName the cluster name
         * @return this Configs instance for method chaining
         */
        public Configs clusterName(final String clusterName) {
            configList.add("-clusterName");
            configList.add(clusterName);
            return this;
        }

        /**
         * Sets the index store type.
         *
         * @param indexStoreType the index store type (e.g., 'fs')
         * @return this Configs instance for method chaining
         */
        public Configs indexStoreType(final String indexStoreType) {
            configList.add("-indexStoreType");
            configList.add(indexStoreType);
            return this;
        }

        /**
         * Enables logger output instead of System.out.
         *
         * @return this Configs instance for method chaining
         */
        public Configs useLogger() {
            configList.add("-useLogger");
            return this;
        }

        /**
         * Disables OpenSearch internal logging.
         *
         * @return this Configs instance for method chaining
         */
        public Configs disableESLogger() {
            configList.add("-disableESLogger");
            return this;
        }

        /**
         * Enables printing stack traces on operation failures.
         *
         * @return this Configs instance for method chaining
         */
        public Configs printOnFailure() {
            configList.add("-printOnFailure");
            return this;
        }

        /**
         * Sets the module types to load.
         *
         * @param moduleTypes comma-separated list of module types
         * @return this Configs instance for method chaining
         */
        public Configs moduleTypes(final String moduleTypes) {
            configList.add("-moduleTypes");
            configList.add(moduleTypes);
            return this;
        }

        /**
         * Sets the plugin types to load.
         *
         * @param pluginTypes comma-separated list of plugin types
         * @return this Configs instance for method chaining
         */
        public Configs pluginTypes(final String pluginTypes) {
            configList.add("-pluginTypes");
            configList.add(pluginTypes);
            return this;
        }

        /**
         * Builds the configuration as a command line arguments array.
         *
         * @return array of command line arguments
         */
        public String[] build() {
            return configList.toArray(new String[configList.size()]);
        }

    }

    private static XContentType xContentType(final CharSequence content) {
        final int length = content.length() < 20 ? content.length() : 20;
        if (length == 0) {
            return null;
        }
        final char first = content.charAt(0);
        if (first == '{') {
            return XContentType.JSON;
        }
        // Should we throw a failure here? Smile idea is to use it in bytes....
        if (length > 2 && first == SmileConstants.HEADER_BYTE_1
                && content.charAt(1) == SmileConstants.HEADER_BYTE_2
                && content.charAt(2) == SmileConstants.HEADER_BYTE_3) {
            return XContentType.SMILE;
        }
        if (length > 2 && first == '-' && content.charAt(1) == '-'
                && content.charAt(2) == '-') {
            return XContentType.YAML;
        }

        // CBOR is not supported

        for (int i = 0; i < length; i++) {
            final char c = content.charAt(i);
            if (c == '{') {
                return XContentType.JSON;
            }
            if (!Character.isWhitespace(c)) {
                break;
            }
        }
        return null;
    }

    /**
     * Callback function for configuring request builders.
     *
     * @param <T> the type of builder to configure
     */
    public interface BuilderCallback<T> {
        /**
         * Applies configuration to the builder.
         *
         * @param builder the builder to configure
         * @return the configured builder
         */
        T apply(T builder);
    }
}

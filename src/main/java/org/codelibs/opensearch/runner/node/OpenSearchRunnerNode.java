/*
 * Copyright 2012-2020 CodeLibs Project and the Others.
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

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.codelibs.opensearch.runner.OpenSearchRunnerException;
import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;

/**
 * Custom Node implementation for running OpenSearch instances in test environments.
 */
public class OpenSearchRunnerNode extends Node {

    private final Collection<Class<? extends Plugin>> plugins;

    /**
     * Constructs a new OpenSearchRunnerNode with the specified environment and plugins.
     *
     * @param tmpEnv the environment configuration
     * @param classpathPlugins the collection of plugin classes to load
     */
    public OpenSearchRunnerNode(final Environment tmpEnv, final Collection<Class<? extends Plugin>> classpathPlugins) {
        super(tmpEnv, classpathPlugins.stream().map(p -> {
            try {
                return new PluginInfo(p.getName(), "classpath plugin", "NA", Version.CURRENT, Integer.toString(Runtime.version().feature()),
                        p.getName(), Collections.emptyList(), false);
            } catch (final Exception e) {
                throw new OpenSearchRunnerException("Failed to create PluginInfo for " + p.getName(), e);
            }
        }).collect(Collectors.toList()), true);
        this.plugins = classpathPlugins;
    }

    /**
     * Gets the collection of plugin classes loaded by this node.
     *
     * @return the plugin classes
     */
    public Collection<Class<? extends Plugin>> getPlugins() {
        return plugins;
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(final ClusterService clusterService) {
        // nothing
    }
}

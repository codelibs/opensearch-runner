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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;

public class OpenSearchRunnerNode extends Node {

    private final Collection<Class<? extends Plugin>> plugins;

    public OpenSearchRunnerNode(final Environment tmpEnv, final Collection<Class<? extends Plugin>> classpathPlugins) {
        super(tmpEnv, classpathPlugins, true);
        this.plugins = classpathPlugins;
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        return plugins;
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(final ClusterService clusterService) {
        // nothing
    }
}

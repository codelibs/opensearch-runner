package org.codelibs.opensearch.runner;

import org.opensearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        OpenSearchRunner runner = new OpenSearchRunner();
        runner.onBuild(new OpenSearchRunner.Builder() {
            @Override
            public void build(int index, Settings.Builder settingsBuilder) {
                settingsBuilder.put("cluster.name", "LiferayOpensearchCluster");
                settingsBuilder.put("network.host", "0.0.0.0");
                settingsBuilder.put("http.port", "9200");
                settingsBuilder.put("transport.tcp.port", "9300");
                settingsBuilder.put("transport.host", "0.0.0.0");
                settingsBuilder.put("transport.bind_host", "0.0.0.0");
                settingsBuilder.put("transport.publish_host", "0.0.0.0");
                settingsBuilder.put("discovery.type", "single-node");

                settingsBuilder.putList("node.roles", "cluster_manager", "data", "ingest", "remote_cluster_client");

                settingsBuilder.put("node.name", "EMDEV-HP");
                settingsBuilder.put("path.plugins", "C:\\Users\\Emdev\\IdeaProjects\\opensearch-runner\\src\\main\\java\\org\\codelibs\\opensearch\\runner\\plugins");
            }
        }).build(OpenSearchRunner.newConfigs()
                .clusterName("LiferayOpensearchCluster")
                .numOfNode(1));

        // wait for status
        runner.ensureGreen();

        System.out.println("!!!! OpenSearch is ready !!!!");
        TimeUnit.MINUTES.sleep(60); // time for check curl.exe -X GET http://localhost:9200
    }
}

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
package org.codelibs.opensearch.runner.net;

import static org.junit.Assert.*;
import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

import org.codelibs.curl.Curl.Method;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.node.Node;

/**
 * Test cases for OpenSearchCurl.
 */
public class OpenSearchCurlTest {

    private static OpenSearchRunner runner;
    private static Node node;

    @BeforeClass
    public static void setUpClass() throws Exception {
        final String clusterName = "curl-test-cluster-" + System.currentTimeMillis();
        runner = new OpenSearchRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
            settingsBuilder.put("http.port", "9201");
        }).build(newConfigs().clusterName(clusterName).numOfNode(1));
        runner.ensureYellow();
        node = runner.node();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (runner != null) {
            runner.close();
            runner.clean();
        }
    }

    @Test
    public void testGetWithNode() {
        final CurlRequest request = OpenSearchCurl.get(node, "/");
        assertNotNull(request);
        assertTrue(request.toString().contains("GET"));
        assertTrue(request.toString().contains("http://localhost:9201/"));
    }

    @Test
    public void testGetWithNodeAndPathWithSlash() {
        final CurlRequest request = OpenSearchCurl.get(node, "/_search");
        assertNotNull(request);
        assertTrue(request.toString().contains("/_search"));
    }

    @Test
    public void testGetWithNodeAndPathWithoutSlash() {
        final CurlRequest request = OpenSearchCurl.get(node, "_search");
        assertNotNull(request);
        assertTrue(request.toString().contains("/_search"));
    }

    @Test
    public void testPostWithNode() {
        final CurlRequest request = OpenSearchCurl.post(node, "/test_index/_doc");
        assertNotNull(request);
        assertTrue(request.toString().contains("POST"));
        assertTrue(request.toString().contains("http://localhost:9201/test_index/_doc"));
    }

    @Test
    public void testPostWithNodeAndPathWithoutSlash() {
        final CurlRequest request = OpenSearchCurl.post(node, "test_index/_doc");
        assertNotNull(request);
        assertTrue(request.toString().contains("/test_index/_doc"));
    }

    @Test
    public void testPutWithNode() {
        final CurlRequest request = OpenSearchCurl.put(node, "/test_index");
        assertNotNull(request);
        assertTrue(request.toString().contains("PUT"));
        assertTrue(request.toString().contains("http://localhost:9201/test_index"));
    }

    @Test
    public void testPutWithNodeAndPathWithoutSlash() {
        final CurlRequest request = OpenSearchCurl.put(node, "test_index");
        assertNotNull(request);
        assertTrue(request.toString().contains("/test_index"));
    }

    @Test
    public void testDeleteWithNode() {
        final CurlRequest request = OpenSearchCurl.delete(node, "/test_index");
        assertNotNull(request);
        assertTrue(request.toString().contains("DELETE"));
        assertTrue(request.toString().contains("http://localhost:9201/test_index"));
    }

    @Test
    public void testDeleteWithNodeAndPathWithoutSlash() {
        final CurlRequest request = OpenSearchCurl.delete(node, "test_index");
        assertNotNull(request);
        assertTrue(request.toString().contains("/test_index"));
    }

    @Test
    public void testGetWithUrl() {
        final String url = "http://localhost:9200/_search";
        final CurlRequest request = OpenSearchCurl.get(url);
        assertNotNull(request);
        assertTrue(request.toString().contains("GET"));
        assertTrue(request.toString().contains(url));
    }

    @Test
    public void testPostWithUrl() {
        final String url = "http://localhost:9200/test/_doc";
        final CurlRequest request = OpenSearchCurl.post(url);
        assertNotNull(request);
        assertTrue(request.toString().contains("POST"));
        assertTrue(request.toString().contains(url));
    }

    @Test
    public void testPutWithUrl() {
        final String url = "http://localhost:9200/test";
        final CurlRequest request = OpenSearchCurl.put(url);
        assertNotNull(request);
        assertTrue(request.toString().contains("PUT"));
        assertTrue(request.toString().contains(url));
    }

    @Test
    public void testDeleteWithUrl() {
        final String url = "http://localhost:9200/test";
        final CurlRequest request = OpenSearchCurl.delete(url);
        assertNotNull(request);
        assertTrue(request.toString().contains("DELETE"));
        assertTrue(request.toString().contains(url));
    }

    @Test
    public void testJsonParser() {
        final Function<CurlResponse, Map<String, Object>> parser =
                OpenSearchCurl.jsonParser();
        assertNotNull(parser);
    }

    @Test
    public void testJsonParserWithValidJson() {
        final Function<CurlResponse, Map<String, Object>> parser =
                OpenSearchCurl.jsonParser();

        final String jsonString = "{\"status\":\"ok\",\"count\":100,\"nested\":{\"key\":\"value\"}}";
        final CurlResponse mockResponse = createMockResponse(jsonString);

        final Map<String, Object> result = parser.apply(mockResponse);
        assertNotNull(result);
        assertEquals("ok", result.get("status"));
        assertEquals(100, result.get("count"));
        assertTrue(result.containsKey("nested"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> nested = (Map<String, Object>) result.get("nested");
        assertEquals("value", nested.get("key"));
    }

    @Test
    public void testJsonParserWithEmptyJson() {
        final Function<CurlResponse, Map<String, Object>> parser =
                OpenSearchCurl.jsonParser();

        final String jsonString = "{}";
        final CurlResponse mockResponse = createMockResponse(jsonString);

        final Map<String, Object> result = parser.apply(mockResponse);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testJsonParserWithArray() {
        final Function<CurlResponse, Map<String, Object>> parser =
                OpenSearchCurl.jsonParser();

        final String jsonString = "{\"items\":[1,2,3,4,5]}";
        final CurlResponse mockResponse = createMockResponse(jsonString);

        final Map<String, Object> result = parser.apply(mockResponse);
        assertNotNull(result);
        assertTrue(result.containsKey("items"));
    }

    @Test(expected = CurlException.class)
    public void testJsonParserWithInvalidJson() {
        final Function<CurlResponse, Map<String, Object>> parser =
                OpenSearchCurl.jsonParser();

        final String invalidJson = "{invalid json}";
        final CurlResponse mockResponse = createMockResponse(invalidJson);

        parser.apply(mockResponse);
    }

    @Test(expected = CurlException.class)
    public void testJsonParserWithNonJsonContent() {
        final Function<CurlResponse, Map<String, Object>> parser =
                OpenSearchCurl.jsonParser();

        final String nonJson = "This is not JSON";
        final CurlResponse mockResponse = createMockResponse(nonJson);

        parser.apply(mockResponse);
    }

    @Test
    public void testUrlConstructionWithComplexPath() {
        final CurlRequest request = OpenSearchCurl.get(node, "/_cluster/health/test_index");
        assertNotNull(request);
        assertTrue(request.toString().contains("/_cluster/health/test_index"));
    }

    @Test
    public void testUrlConstructionWithQueryString() {
        final CurlRequest request = OpenSearchCurl.get(node, "/_search?q=*:*");
        assertNotNull(request);
        assertTrue(request.toString().contains("/_search?q=*:*"));
    }

    @Test
    public void testAllMethodsReturnNonNullRequests() {
        assertNotNull(OpenSearchCurl.get(node, "/"));
        assertNotNull(OpenSearchCurl.post(node, "/"));
        assertNotNull(OpenSearchCurl.put(node, "/"));
        assertNotNull(OpenSearchCurl.delete(node, "/"));

        assertNotNull(OpenSearchCurl.get("http://localhost:9200/"));
        assertNotNull(OpenSearchCurl.post("http://localhost:9200/"));
        assertNotNull(OpenSearchCurl.put("http://localhost:9200/"));
        assertNotNull(OpenSearchCurl.delete("http://localhost:9200/"));
    }

    @Test
    public void testGetUrlWithEmptyPath() {
        final CurlRequest request = OpenSearchCurl.get(node, "");
        assertNotNull(request);
        assertTrue(request.toString().contains("http://localhost:9201/"));
    }

    @Test
    public void testIntegrationWithRealCluster() throws Exception {
        // Test actual HTTP request to the running cluster
        try (CurlResponse response = OpenSearchCurl.get(node, "/")
                .header("Content-Type", "application/json")
                .execute()) {
            assertNotNull(response);
            final Map<String, Object> content =
                    response.getContent(OpenSearchCurl.jsonParser());
            assertNotNull(content);
            assertTrue(content.containsKey("cluster_name"));
            assertTrue(content.containsKey("version"));
        }
    }

    @Test
    public void testIntegrationCreateAndDeleteIndex() throws Exception {
        final String testIndex = "curl_test_index_" + System.currentTimeMillis();

        // Create index
        try (CurlResponse response = OpenSearchCurl.put(node, "/" + testIndex)
                .header("Content-Type", "application/json")
                .body("{\"settings\":{\"number_of_shards\":1}}")
                .execute()) {
            final Map<String, Object> content =
                    response.getContent(OpenSearchCurl.jsonParser());
            assertNotNull(content);
            assertEquals(true, content.get("acknowledged"));
        }

        // Delete index
        try (CurlResponse response = OpenSearchCurl.delete(node, "/" + testIndex)
                .header("Content-Type", "application/json")
                .execute()) {
            final Map<String, Object> content =
                    response.getContent(OpenSearchCurl.jsonParser());
            assertNotNull(content);
            assertEquals(true, content.get("acknowledged"));
        }
    }

    /**
     * Helper method to create a mock CurlResponse with given JSON content.
     */
    private CurlResponse createMockResponse(final String jsonContent) {
        return new CurlResponse() {
            @Override
            public int getHttpStatusCode() {
                return 200;
            }

            @Override
            public String getContentAsString() {
                return jsonContent;
            }

            @Override
            public InputStream getContentAsStream() {
                return new ByteArrayInputStream(
                        jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            @Override
            public void close() {
                // No-op for mock
            }
        };
    }
}

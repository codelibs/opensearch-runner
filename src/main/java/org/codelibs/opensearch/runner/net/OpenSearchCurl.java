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

import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

import org.codelibs.curl.Curl.Method;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.node.Node;

public class OpenSearchCurl {

    protected OpenSearchCurl() {
        // nothing
    }

    public static CurlRequest get(final Node node, final String path) {
        return new CurlRequest(Method.GET, getUrl(node, path));
    }

    public static CurlRequest post(final Node node, final String path) {
        return new CurlRequest(Method.POST, getUrl(node, path));
    }

    public static CurlRequest put(final Node node, final String path) {
        return new CurlRequest(Method.PUT, getUrl(node, path));
    }

    public static CurlRequest delete(final Node node, final String path) {
        return new CurlRequest(Method.DELETE, getUrl(node, path));
    }

    protected static String getUrl(final Node node, final String path) {
        final StringBuilder urlBuf = new StringBuilder(200);
        urlBuf.append("http://localhost:")
                .append(node.settings().get("http.port"));
        if (path.startsWith("/")) {
            urlBuf.append(path);
        } else {
            urlBuf.append('/').append(path);
        }
        return urlBuf.toString();
    }

    public static CurlRequest get(final String url) {
        return new CurlRequest(Method.GET, url);
    }

    public static CurlRequest post(final String url) {
        return new CurlRequest(Method.POST, url);
    }

    public static CurlRequest put(final String url) {
        return new CurlRequest(Method.PUT, url);
    }

    public static CurlRequest delete(final String url) {
        return new CurlRequest(Method.DELETE, url);
    }

    public static Function<CurlResponse, Map<String, Object>> jsonParser() {
        return PARSER;
    }

    protected static final Function<CurlResponse, Map<String, Object>> PARSER = response -> {
        try (InputStream is = response.getContentAsStream()) {
            return JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE, is)
                    .map();
        } catch (final Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    };

}

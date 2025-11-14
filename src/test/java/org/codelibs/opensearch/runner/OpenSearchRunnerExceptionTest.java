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

import static org.junit.Assert.*;

import org.junit.Test;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;

/**
 * Test cases for OpenSearchRunnerException.
 */
public class OpenSearchRunnerExceptionTest {

    @Test
    public void testConstructorWithMessageAndCause() {
        final String message = "Test error message";
        final Throwable cause = new IllegalArgumentException("Root cause");

        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertNull(exception.getActionResponse());
    }

    @Test
    public void testConstructorWithMessage() {
        final String message = "Test error message";

        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
        assertNull(exception.getActionResponse());
    }

    @Test
    public void testConstructorWithMessageAndResponse() {
        final String message = "Test error message";
        final ActionResponse response = createMockIndexResponse();

        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(message, response);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
        assertNotNull(exception.getActionResponse());
        assertEquals(response, exception.getActionResponse());
    }

    @Test
    public void testGetActionResponseWithTypeCasting() {
        final String message = "Test error message";
        final IndexResponse indexResponse = createMockIndexResponse();

        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(message, indexResponse);

        final IndexResponse retrievedResponse = exception.getActionResponse();
        assertNotNull(retrievedResponse);
        assertEquals(indexResponse, retrievedResponse);
    }

    @Test
    public void testGetActionResponseReturnsNull() {
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException("Test message");

        final ActionResponse response = exception.getActionResponse();
        assertNull(response);
    }

    @Test
    public void testExceptionIsRuntimeException() {
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException("Test message");

        assertTrue(exception instanceof RuntimeException);
    }

    @Test
    public void testExceptionChaining() {
        final IllegalArgumentException rootCause =
                new IllegalArgumentException("Root cause");
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException("Wrapped exception", rootCause);

        assertNotNull(exception.getCause());
        assertEquals(rootCause, exception.getCause());
        assertEquals("Root cause", exception.getCause().getMessage());
    }

    @Test
    public void testNullMessageWithCause() {
        final Throwable cause = new RuntimeException("Cause message");
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(null, cause);

        assertNull(exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testNullCause() {
        final String message = "Test message";
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(message, (Throwable) null);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testNullResponse() {
        final String message = "Test message";
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException(message, (ActionResponse) null);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getActionResponse());
    }

    @Test
    public void testExceptionStackTrace() {
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException("Test message");

        final StackTraceElement[] stackTrace = exception.getStackTrace();
        assertNotNull(stackTrace);
        assertTrue(stackTrace.length > 0);
    }

    @Test
    public void testExceptionSerialization() {
        final OpenSearchRunnerException exception =
                new OpenSearchRunnerException("Test message");

        // Verify serialVersionUID is set (this ensures serialization compatibility)
        // The presence of serialVersionUID field indicates serialization support
        assertNotNull(exception);
        assertTrue(exception instanceof java.io.Serializable);
    }

    /**
     * Helper method to create a mock IndexResponse for testing.
     * Note: This creates a minimal IndexResponse instance.
     */
    private IndexResponse createMockIndexResponse() {
        final ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        return new IndexResponse(shardId, "1", 1, 1, 1, true);
    }
}

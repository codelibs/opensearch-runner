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

import org.opensearch.core.action.ActionResponse;

/**
 * Custom exception for OpenSearchRunner operations.
 */
public class OpenSearchRunnerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient ActionResponse response;

    /**
     * Constructs a new exception with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public OpenSearchRunnerException(final String message,
            final Throwable cause) {
        super(message, cause);
        this.response = null;
    }

    /**
     * Constructs a new exception with the specified message.
     *
     * @param message the detail message
     */
    public OpenSearchRunnerException(final String message) {
        super(message);
        this.response = null;
    }

    /**
     * Constructs a new exception with the specified message and response.
     *
     * @param message the detail message
     * @param response the action response that caused the exception
     */
    public OpenSearchRunnerException(final String message,
            final ActionResponse response) {
        super(message);
        this.response = response;
    }

    /**
     * Gets the action response associated with this exception.
     *
     * @param <T> the type of action response
     * @return the action response, or null if none was provided
     */
    @SuppressWarnings("unchecked")
    public <T extends ActionResponse> T getActionResponse() {
        return (T) response;
    }
}

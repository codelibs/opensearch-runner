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

import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;

import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.simple.SimpleLoggerContext;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;

import junit.framework.TestCase;

/**
 * Tests that a foreign Log4j2 LoggerContextFactory, such as the one installed
 * by log4j-to-slf4j, does not prevent a cluster from starting. See
 * <a href="https://github.com/codelibs/opensearch-runner/issues/9">issue #9</a>.
 */
public class OpenSearchRunnerLoggerContextFactoryTest extends TestCase {

    /**
     * Stands in for org.apache.logging.slf4j.SLF4JLoggerContextFactory: a valid
     * factory whose contexts OpenSearch cannot use, because they are not
     * org.apache.logging.log4j.core.LoggerContext instances.
     */
    private static class ForeignContextFactory
            implements LoggerContextFactory {

        private final SimpleLoggerContext context = new SimpleLoggerContext();

        @Override
        public LoggerContext getContext(final String fqcn,
                final ClassLoader loader, final Object externalContext,
                final boolean currentContext) {
            return context;
        }

        @Override
        public LoggerContext getContext(final String fqcn,
                final ClassLoader loader, final Object externalContext,
                final boolean currentContext, final URI configLocation,
                final String name) {
            return context;
        }

        @Override
        public void removeContext(final LoggerContext context) {
            // nothing to remove
        }
    }

    private LoggerContextFactory originalContextFactory;

    private ForeignContextFactory foreignContextFactory;

    private OpenSearchRunner runner;

    @Override
    protected void setUp() throws Exception {
        originalContextFactory = LogManager.getFactory();
        foreignContextFactory = new ForeignContextFactory();
        LogManager.setFactory(foreignContextFactory);
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (runner != null) {
                runner.close();
                runner.clean();
            }
        } finally {
            LogManager.setFactory(originalContextFactory);
        }
    }

    public void test_startsClusterAndRestoresContextFactory() throws Exception {
        runner = new OpenSearchRunner();
        runner.build(newConfigs()
                .clusterName("es-cf-run-" + System.currentTimeMillis())
                .numOfNode(1));
        runner.ensureYellow();

        assertTrue(
                "log4j-core must be the active provider while a cluster runs, but was "
                        + LogManager.getFactory().getClass().getName(),
                LogManager.getFactory() instanceof Log4jContextFactory);

        runner.close();

        assertSame("The original factory must be restored on close",
                foreignContextFactory, LogManager.getFactory());

        runner.clean();
        runner = null;
    }

    public void test_keepLoggerContextFactory_failsWithDescriptiveMessage() {
        runner = new OpenSearchRunner();
        try {
            runner.build(newConfigs()
                    .clusterName("es-cf-run-" + System.currentTimeMillis())
                    .numOfNode(1).keepLoggerContextFactory());
            fail("A descriptive exception must be thrown.");
        } catch (final OpenSearchRunnerException e) {
            final String message = e.getMessage();
            assertTrue("The message must name the active factory: " + message,
                    message.contains(ForeignContextFactory.class.getName()));
            assertTrue("The message must suggest a workaround: " + message,
                    message.contains("log4j2.loggerContextFactory"));
        } finally {
            runner = null;
        }
        assertSame("The foreign factory must be left untouched",
                foreignContextFactory, LogManager.getFactory());
    }
}

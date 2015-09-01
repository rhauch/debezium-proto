/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ServerTest implements Testing {

    private Server server;
    private File dataDir;

    @Before
    public void beforeEach() {
        resetBeforeEachTest();
        dataDir = Testing.Files.createTestingDirectory("server");
        Testing.Files.delete(dataDir);
        server = new Server().usingDirectory(dataDir);
    }

    @After
    public void afterEach() {
        server.shutdown(20, TimeUnit.SECONDS);
        Testing.Files.delete(dataDir);
    }

    @Test
    public void shouldStartServer() throws IOException {
        server.startup();
        assertThat(server.isRunning()).isTrue();
    }

}

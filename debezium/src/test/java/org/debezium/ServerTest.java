/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ServerTest implements Testing {

    private Server server;

    @Before
    public void beforeEach() {
        resetBeforeEachTest();
        server = new Server().usingDirectory(Testing.Files.createTestingDirectory("server"))
                             .addBrokers(1)
                             .deleteDataUponShutdown(true)
                             .deleteDataPriorToStartup(true);
    }

    @After
    public void afterEach() {
        server.shutdown();
    }

    @Test
    public void shouldStartServer() throws IOException, InterruptedException {
        Testing.Print.enable();
        server.startup();
        Thread.sleep(2500);
        assertThat(server.isRunning()).isTrue();
        Testing.print("BEGINNING SHUTDOWN");
    }

}

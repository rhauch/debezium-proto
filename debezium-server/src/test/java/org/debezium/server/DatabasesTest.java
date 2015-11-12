/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.io.File;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.debezium.Server;
import org.debezium.Testing;
import org.debezium.driver.Environment;
import org.debezium.driver.MockSecurityProvider;
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class DatabasesTest {

    private static UndertowJaxrsServer server;
    private static Server embeddedServices;

    @BeforeClass
    public static void beforeAll() throws Exception {
        // Start the Debezium services (including Kafka and Zookeeper) and the JAX-RS server
        File dataDir = Testing.Files.createTestingDirectory("server");
        embeddedServices = new Server().usingDirectory(dataDir)
                                       .addBrokers(1)
                                       .deleteDataUponShutdown(true)
                                       .deleteDataPriorToStartup(true)
                                       .startup();
        server = new UndertowJaxrsServer().start();

        // Now create and deploy the application, using a mock security provider ...
        DebeziumV1Application app = new DebeziumV1Application(Environment.build().withSecurity(MockSecurityProvider::new));
        server.deploy(app);
    }

    @AfterClass
    public static void afterAll() throws Exception {
        try {
            server.stop();
        } finally {
            embeddedServices.shutdown();
        }
    }

    private Client client;

    @Before
    public void beforeEach() {
        client = ClientBuilder.newClient();
    }

    @After
    public void afterEach() {
        client.close();
    }

    protected WebTarget request(String url) {
        return client.target(TestPortProvider.generateURL(url));
    }

    // @Test
    // public void shouldReturnListOfAccessibleDatabases() {
    // String result = request("/api/v1/db").request().get(String.class);
    // assertThat(result).isEqualTo("hello world");
    // }
    //
    // @Test
    // public void shouldReturnSchemaOfAccessibleDatabases() {
    // String result = request("/api/v1/db/1").request().get(String.class);
    // assertThat(result).isEqualTo("Hello, World");
    // }

    @Test
    public void shouldReturnEntityById() throws Exception {
        System.out.println("Sleeping ...");
        Thread.sleep(1000);
        System.out.println("Done sleeping!");
        
//        String result = request("/api/v1/db/1/t/contact/e/200").request().get(String.class);
//        assertThat(result).isEqualTo("hello, entity 200 of type 'contact' in database 1!");
    }
}

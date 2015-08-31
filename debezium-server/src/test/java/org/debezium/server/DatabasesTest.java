/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.debezium.driver.Debezium;
import org.debezium.driver.EmbeddedDebeziumServices;
import org.debezium.driver.MockSecurityProvider;
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class DatabasesTest {

    private static UndertowJaxrsServer server;
    private static EmbeddedDebeziumServices services;

    private static void configureClient(Debezium.Builder clientBuilder) {
        clientBuilder.usingBus(services.supplyMessageBus())
                     .usingExecutor(services.supplyExecutorService())
                     .usingScheduledExecutor(services.supplyScheduledExecutorService())
                     .usingSecurity(MockSecurityProvider::new);
    }

    @BeforeClass
    public static void beforeAll() throws Exception {
        server = new UndertowJaxrsServer().start();
        services = new EmbeddedDebeziumServices();
        DebeziumV1Application app = new DebeziumV1Application(DatabasesTest::configureClient);
        server.deploy(app);
    }

    @AfterClass
    public static void afterAll() throws Exception {
        try {
            server.stop();
        } finally {
            services.shutdown(10, TimeUnit.SECONDS);
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

//    @Test
//    public void shouldReturnListOfAccessibleDatabases() {
//        String result = request("/api/v1/db").request().get(String.class);
//        assertThat(result).isEqualTo("hello world");
//    }
//
//    @Test
//    public void shouldReturnSchemaOfAccessibleDatabases() {
//        String result = request("/api/v1/db/1").request().get(String.class);
//        assertThat(result).isEqualTo("Hello, World");
//    }

    @Test
    public void shouldReturnEntityById() {
        String result = request("/api/v1/db/1/t/contact/e/200").request().get(String.class);
        assertThat(result).isEqualTo("hello, entity 200 of type 'contact' in database 1!");
    }
}

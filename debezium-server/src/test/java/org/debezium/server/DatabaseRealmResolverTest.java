/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.debezium.driver.DebeziumAuthorizationException;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.adapters.KeycloakDeployment;
import org.slf4j.helpers.NOPLogger;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Fail.fail;

public class DatabaseRealmResolverTest {

    private static final String DIRECTORY_PATH = new File("src/test/resources").getAbsolutePath();
    private DatabaseRealmResolver resolver;

    @Before
    public void beforeEach() {
        resolver = new DatabaseRealmResolver(DIRECTORY_PATH, null, null, System::currentTimeMillis, NOPLogger.NOP_LOGGER);
    }

    @Test
    public void shouldExtractDatabaseFromValidUris() {
        assertThat(resolver.databaseId("db/1")).isEqualTo("1");
        assertThat(resolver.databaseId("db/a")).isEqualTo("a");
        assertThat(resolver.databaseId("db/_")).isEqualTo("_");
        assertThat(resolver.databaseId("db/a/b/c")).isEqualTo("a");
        assertThat(resolver.databaseId("db/a?b=c")).isEqualTo("a");
        assertThat(resolver.databaseId("db/alpha-beta-gamma/b/c")).isEqualTo("alpha-beta-gamma");
        assertThat(resolver.databaseId("db/alpha-beta-gamma?b=c")).isEqualTo("alpha-beta-gamma");

        assertThat(resolver.databaseId("/db/1")).isEqualTo("1");
        assertThat(resolver.databaseId("/db/a")).isEqualTo("a");
        assertThat(resolver.databaseId("/db/_")).isEqualTo("_");
        assertThat(resolver.databaseId("/db/a/b/c")).isEqualTo("a");
        assertThat(resolver.databaseId("/db/a?b=c")).isEqualTo("a");
        assertThat(resolver.databaseId("/db/alpha-beta-gamma/b/c")).isEqualTo("alpha-beta-gamma");
        assertThat(resolver.databaseId("/db/alpha-beta-gamma?b=c")).isEqualTo("alpha-beta-gamma");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToExtractDatabaseFromInvalidUriWithoutPrefix() {
        resolver.databaseId("d");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToExtractDatabaseFromInvalidUriWithJustPrefix() {
        resolver.databaseId("db");
    }

    @Test
    public void shouldLoadFileViaClasspathWhenNotFoundOnFileSystem() throws IOException {
        // The log4j.properties file is on the classpath but not accessible via the file system path ...
        try (InputStream stream = resolver.loadFile("log4j.properties")) {
            assertThat(stream).isNotNull();
        }
    }

    @Test
    public void shouldLoadFileViaFileSystem() throws IOException {
        // The "webapp/WEB_INF/web.xml" file is not available on the classpath, so if we find it we know the FS worked ...
        resolver = new DatabaseRealmResolver(new File("src/main").getAbsolutePath(), null, null);
        try (InputStream stream = resolver.loadFile("webapp/WEB-INF/web.xml")) {
            assertThat(stream).isNotNull();
        }
    }

    @Test
    public void shouldDetermineRealmConfigFileName() {
        assertThat(resolver.getRealmFilename("realm-ABC")).isEqualTo(DatabaseRealmResolver.DEFAULT_PREFIX + "realm-ABC-keycloak.json");
    }

    @Test
    public void shouldResolveTestDatabases() {
        for (int i = 0; i != 10; ++i) {
            assertRealm1(resolver.resolve("db/db1"));
            assertRealm1(resolver.resolve("db/db3"));
            assertRealm2(resolver.resolve("db/db2"));
            assertRealm2(resolver.resolve("db/db4"));
        }
        assertThat(resolver.countRealmConfigurationLoads()).isEqualTo(2);
        assertThat(resolver.countDatabaseMappingLoads()).isEqualTo(1);
    }

    @Test(expected = DebeziumAuthorizationException.class)
    public void shouldFailResolveNonExistantDatabases() {
        resolver.resolve("db/does-not-exist");
    }

    @Test
    public void shouldNotReloadDatabasesIfWithinInterval() {
        AtomicLong clock = new AtomicLong(1000);
        resolver = new DatabaseRealmResolver(DIRECTORY_PATH, null, null, clock::get, NOPLogger.NOP_LOGGER);
        assertThat(resolver.countDatabaseMappingLoads()).isEqualTo(0);
        for (int i = 0; i != 100; ++i) {
            try {
                resolver.resolve("db/non-existant");
                fail("Should not happen!");
            } catch (DebeziumAuthorizationException e) {
                // expected
            }
            clock.incrementAndGet();
        }
        assertThat(resolver.countDatabaseMappingLoads()).isEqualTo(1);
        // Advance the clock beyond the interval ...
        clock.addAndGet(TimeUnit.DAYS.toMillis(1));
        try {
            resolver.resolve("db/non-existant");
            fail("Should not happen!");
        } catch (DebeziumAuthorizationException e) {
            // expected
        }
        assertThat(resolver.countDatabaseMappingLoads()).isEqualTo(2);
    }

    @Test
    public void shouldFailResolveNonExistantRealm() {
        assertThat(resolver.realmName("does-not-exist")).isNull();
    }

    protected void assertRealm1(KeycloakDeployment deployment) {
        assertThat(deployment.getRealm()).isEqualTo("tenant1");
        assertThat(deployment.getResourceName()).isEqualTo("multi-tenant");
        assertThat(deployment.getAuthServerBaseUrl()).isEqualTo("http://remote:8080/auth");
    }

    protected void assertRealm2(KeycloakDeployment deployment) {
        assertThat(deployment.getRealm()).isEqualTo("tenant2");
        assertThat(deployment.getResourceName()).isEqualTo("multi-tenant");
        assertThat(deployment.getAuthServerBaseUrl()).isEqualTo("http://localhost:8080/auth");
    }
}

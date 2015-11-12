/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import org.debezium.driver.DebeziumAuthorizationException;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.spi.HttpFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link KeycloakConfigResolver} implementation that identifies the appropriate Keycloak realm for each database.
 * This should be used when a single Debezium server has multiple independent tenants.
 * <p>
 * This resolver evaluates the URI of the requested page, and works for those URIs that begin with "<code>db/{dbId}</code>",
 * where "<code>{dbId}</code> is the identifier of the database being accessed.
 * <p>
 * Each database is mapped to a realm name via a properties file, where each property in that file has a name matching the
 * database ID and whose value is the name of the realm. Once the realm name is found, the Keycloak deployment for that realm is
 * then loaded from a corresponding file for the realm.
 * <p>
 * The logic of finding these files defaults to first looking on the file system under the
 * "<code>${jboss.server.data.dir}/debezium/realms</code>" directory, and if not found then accessing the files from the
 * classpath.
 * 
 * @author Randall Hauch
 */
public class DatabaseRealmResolver implements KeycloakConfigResolver {

    public static final String DEFAULT_PREFIX = "/debezium/realms/";
    public static final String DEFAULT_SUFFIX = "-keycloak.json";
    public static final String DB_REALMS_FILENAME = "database_realms.properties";

    private final Logger logger;
    private final String prefix;
    private final String suffix;
    private final String directoryPath;
    private final ConcurrentMap<String, KeycloakDeployment> cacheByRealm = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, KeycloakDeployment> cacheByDb = new ConcurrentHashMap<>();
    private final Lock realToDbLock = new ReentrantLock();
    private volatile Properties realmNameByDb = new Properties();
    private final AtomicLong realmLoads = new AtomicLong();
    private final AtomicLong mappingLoads = new AtomicLong();
    private final LongSupplier clock;
    private final long dbLoadIntervalInMillis = TimeUnit.SECONDS.toMillis(30L);
    private volatile long earliestLoadTime = -1L;

    public DatabaseRealmResolver() {
        this(null, null, null);
    }

    protected DatabaseRealmResolver(String directoryPath, String prefix, String suffix) {
        this(directoryPath, prefix, suffix, System::currentTimeMillis, null);
    }

    protected DatabaseRealmResolver(String directoryPath, String prefix, String suffix, LongSupplier currentTime, Logger logger ) {
        this.prefix = prefix != null ? prefix : DEFAULT_PREFIX;
        this.suffix = suffix != null ? suffix : DEFAULT_SUFFIX;
        this.directoryPath = directoryPath != null ? directoryPath : System.getProperty("jboss.server.data.dir");
        this.logger = logger != null ? logger : LoggerFactory.getLogger(getClass());
        this.clock = currentTime;
    }

    protected String databaseId(String requestUri) {
        int dbIndex = requestUri.indexOf("db/");
        if (dbIndex == -1) {
            throw new IllegalArgumentException("Not able to resolve database ID from the request path!");
        }
        String dbId = requestUri.substring(dbIndex).split("/")[1];
        if (dbId.contains("?")) {
            dbId = dbId.split("\\?")[0];
        }
        return dbId;
    }

    @Override
    public KeycloakDeployment resolve(HttpFacade.Request facade) {
        return resolve(facade.getURI());
    }

    public KeycloakDeployment resolve(String uri) {
        String dbId = databaseId(uri);
        KeycloakDeployment deployment = cacheByDb.get(dbId);
        if (deployment == null) {
            // Find the realm name and its deployment ...
            String realmName = realmName(dbId);
            if (realmName == null) {
                // There is no realm for this database ..
                logger.error("Failed to find realm for database '{}'", dbId);
                throw new DebeziumAuthorizationException();
            }
            deployment = cacheByRealm.get(realmName);
            if (deployment == null) {
                // We have to read in the deployment ...
                try (InputStream stream = loadRealmConfiguration(realmName)) {
                    if (stream == null) {
                        logger.error("Failed to find configuration for realm '{}' for database '{}'", realmName, dbId);
                        throw new DebeziumAuthorizationException();
                    }
                    realmLoads.incrementAndGet();
                    deployment = KeycloakDeploymentBuilder.build(stream);
                    cacheByRealm.put(realmName, deployment);
                } catch (IOException e) {
                    throw new IllegalStateException("Unable to read the configuration for realm '" + realmName + "'");
                }
            }
            cacheByDb.put(dbId, deployment);
        }
        return deployment;
    }

    protected String realmName(String dbId) {
        String realmName = realmNameByDb.getProperty(dbId);
        if (realmName == null) {
            try {
                realToDbLock.lock();
                realmName = realmNameByDb.getProperty(dbId);
                if (realmName == null) {
                    // We don't want to reload the file too often ...
                    long currentTime = clock.getAsLong();
                    if ( currentTime >= earliestLoadTime ) {
                        // Reload the file to see if the database is there ...
                        try (InputStream stream = loadDatabaseToRealmMapping()) {
                            if (stream == null) {
                                throw new IllegalStateException("Unable to find the database to realms mapping file");
                            }
                            mappingLoads.incrementAndGet();
                            Properties mapping = new Properties();
                            mapping.load(stream);
                            realmNameByDb = mapping;
                        } catch (IOException e) {
                            throw new IllegalStateException("Unable to read the database to realms mapping file");
                        }
                        earliestLoadTime = currentTime + dbLoadIntervalInMillis;
                    }
                }
            } finally {
                realToDbLock.unlock();
            }
            realmName = realmNameByDb.getProperty(dbId);
        }
        return realmName;
    }

    protected InputStream loadRealmConfiguration(String realmName) {
        return loadFile(getRealmFilename(realmName));
    }

    protected InputStream loadDatabaseToRealmMapping() {
        return loadFile(getDatabaseToRealmMappingFilename());
    }

    protected InputStream loadFile(String filename) {
        // First look in the data directory ...
        File dataDir = new File(this.directoryPath);
        if (dataDir.isDirectory() && dataDir.exists()) {
            File file = new File(dataDir, filename);
            if (file.isFile() && file.exists()) {
                try {
                    return new BufferedInputStream(new FileInputStream(file));
                } catch (FileNotFoundException e) {
                    // Shouldn't happen, but if it does just continue ...
                }
            }
        }
        // Otherwise look on the classpath ...
        return getClass().getClassLoader().getResourceAsStream(filename);
    }

    protected String getRealmFilename(String realmName) {
        return prefix + realmName + suffix;
    }

    protected String getDatabaseToRealmMappingFilename() {
        return prefix + DB_REALMS_FILENAME;
    }
    
    protected long countRealmConfigurationLoads() {
        return realmLoads.get();
    }
    
    protected long countDatabaseMappingLoads() {
        return mappingLoads.get();
    }
}

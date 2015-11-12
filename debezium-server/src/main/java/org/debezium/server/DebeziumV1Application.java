/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.debezium.Configuration;
import org.debezium.annotation.Immutable;
import org.debezium.driver.DebeziumDriver;
import org.debezium.driver.Environment;
import org.debezium.driver.PassthroughSecurityProvider;
import org.debezium.message.ArrayReader;
import org.debezium.message.ArrayWriter;
import org.debezium.message.DocumentReader;
import org.debezium.message.DocumentWriter;
import org.debezium.server.io.ArrayBodyReader;
import org.debezium.server.io.ArrayBodyWriter;
import org.debezium.server.io.DocumentBodyReader;
import org.debezium.server.io.DocumentBodyWriter;
import org.debezium.server.io.PatchBodyWriter;
import org.debezium.util.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The JAX-RS {@link Application} that represents version 1 of the Debezium API.
 * 
 * @author Randall Hauch
 */
@Immutable
@ApplicationPath("/api/v1")
public final class DebeziumV1Application extends Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumV1Application.class);

    private static final boolean PRETTY_PRINT = false;

    private final Set<Object> singletons;
    private final Set<Class<?>> classes;

    public DebeziumV1Application() {
        // We can use the pass-through provider because we rely upon the server (in which we are deployed) using OAuth 2.0 to
        // authorize and authenticate users for the database.
        this(Environment.build().withSecurity(PassthroughSecurityProvider::new));
    }

    DebeziumV1Application( Environment.Builder envBuilder ) {
        Set<Object> objs = new HashSet<>();
        if (PRETTY_PRINT) {
            objs.add(new DocumentBodyWriter(DocumentWriter.prettyWriter()));
            objs.add(new ArrayBodyWriter(ArrayWriter.prettyWriter()));
            objs.add(new PatchBodyWriter(ArrayWriter.prettyWriter()));
        } else {
            objs.add(new DocumentBodyWriter(DocumentWriter.defaultWriter()));
            objs.add(new ArrayBodyWriter(ArrayWriter.defaultWriter()));
            objs.add(new PatchBodyWriter(ArrayWriter.defaultWriter()));
        }
        objs.add(new DocumentBodyReader(DocumentReader.defaultReader()));
        objs.add(new ArrayBodyReader(ArrayReader.defaultReader()));
        objs.add(new DebeziumExceptionMapper());

        try {
            // Set up the Debezium driver ...
            Configuration config = Configuration.load("debezium.properties", getClass())
                                                .withSystemProperties("DEBEZIUM_");
            DebeziumDriver driver = DebeziumDriver.create(config, envBuilder.create());

            // Create the singleton used to handle requests ...
            final long defaultTimeout = driver.getConfiguration().getLong("service.max.timeout.ms", 10000);
            objs.add(new Databases(driver, defaultTimeout, TimeUnit.MILLISECONDS));
        } catch (IOException e) {
            LOGGER.error("Error while reading 'debezium.properties' from classpath: {}", e.getMessage(), e);
        }
        
        singletons = Collect.unmodifiableSet(objs);
        classes = Collect.unmodifiableSet();
    }
    
    

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

    @Override
    public Set<Class<?>> getClasses() {
        return classes;
    }
}

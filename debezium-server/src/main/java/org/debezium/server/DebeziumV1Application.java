/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.doc.ArrayReader;
import org.debezium.core.doc.ArrayWriter;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.doc.DocumentWriter;
import org.debezium.core.util.Collect;
import org.debezium.core.util.IoUtil;
import org.debezium.driver.Debezium;
import org.debezium.driver.PassthroughSecurityProvider;
import org.debezium.driver.SecurityProvider;
import org.debezium.server.io.ArrayBodyReader;
import org.debezium.server.io.ArrayBodyWriter;
import org.debezium.server.io.DocumentBodyReader;
import org.debezium.server.io.DocumentBodyWriter;
import org.debezium.server.io.PatchBodyWriter;
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
    private final Set<Class<?>> classes = Collect.unmodifiableSet();

    public DebeziumV1Application() {
        this(null);
    }
    
    protected DebeziumV1Application( Consumer<Debezium.Builder> configurationBuilder ) {
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
        
        // Set up the security provider for the driver ...
        final SecurityProvider security = new PassthroughSecurityProvider();

        // Set up the Debezium driver ...
        Debezium driver = Debezium.driver()
                                  .clientIdPrefix("apiv1")
                                  .load(this::configuration) // load the default properties (which can be overridden)
                                  .loadFromSystemProperties() // then load from the system properties (which are easy to override)
                                  .accept(configurationBuilder)
                                  .usingSecurity(()->security)
                                  .start();
        final long defaultTimeout = driver.getConfiguration().getLong("service.max.timeout.ms", 10000);
        objs.add(new Databases(driver,defaultTimeout,TimeUnit.MILLISECONDS));
        singletons = Collect.unmodifiableSet(objs);
    }

    private Properties configuration() {
        Properties props = new Properties();
        try (InputStream stream = IoUtil.getResourceAsStream("debezium.properties", null, getClass(), null, null)) {
            if (stream != null) {
                props.load(stream);
            }
        } catch (IOException e) {
            LOGGER.error("Error while reading 'debezium.properties' from classpath: {}", e.getMessage(), e);
        }
        return props;
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

/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Randall Hauch
 *
 */
public final class Debezium {

    private static final Properties bundleProperties = loadBundleProperties();

    private static Properties loadBundleProperties() {
        // This is idempotent, so we don't need to lock ...
        try ( InputStream stream = Debezium.class.getClassLoader().getResourceAsStream("org/debezium/build.properties") ){
            Properties props = new Properties();
            props.load(stream);
            return props;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to find the Debezium build definition file",e);
        }
    }

    /**
     * Get the version suitable for public display.
     * 
     * @return the name; never null
     */
    public static final String getVersion() {
        return bundleProperties.getProperty("version");
    }

}
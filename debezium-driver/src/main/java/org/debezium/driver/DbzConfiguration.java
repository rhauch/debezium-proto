/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Properties;

import org.debezium.core.doc.Document;

/**
 * @author Randall Hauch
 *
 */
final class DbzConfiguration implements Configuration {
    
    public static final String CONSUMER_SECTION = "consumers";
    public static final String PRODUCER_SECTION = "producers";
    public static final String INIT_PRODUCER_LAZILY = "initProducerLazily";

    private final Document config;
    
    DbzConfiguration( Document config ) {
        this.config = config;
    }
    
    Document getDocument() {
        return config.clone();
    }
    
    @Override
    public String toString() {
        return config.toString();
    }
    
    static Properties asProperties(Document doc) {
        Properties props = new Properties();
        if ( doc != null ) doc.forEach(field -> {
            props.put(field.getName(), field.getValue().convert().asString());
        });
        return props;
    }
    
}

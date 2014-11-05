/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.util.Properties;

import org.debezium.core.doc.Document;

/**
 * @author Randall Hauch
 *
 */
final class DbzConfiguration implements Configuration {
    
    public static final String CONSUMER_SECTION = "consumer";
    public static final String PRODUCER_SECTION = "producer";

    private final Document config;
    
    DbzConfiguration( Document config ) {
        this.config = config;
    }
    
    Document getDocument() {
        return config.clone();
    }
    
    static Properties asProperties(Document doc) {
        Properties props = new Properties();
        if ( doc != null ) doc.forEach(field -> props.put(field.getName(), field.getValue().convert().asString()));
        return props;
    }
    
}

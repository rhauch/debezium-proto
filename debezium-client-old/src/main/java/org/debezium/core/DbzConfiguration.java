/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.Properties;

import org.debezium.api.Debezium;

/**
 * @author Randall Hauch
 *
 */
final class DbzConfiguration implements Debezium.Configuration {

    
    private final Properties producerProperties;
    private final Properties consumerProperties;
    
    DbzConfiguration( Properties producerProperties, Properties consumerProperties ) {
        this.producerProperties = producerProperties;
        this.consumerProperties = consumerProperties;
    }
    
    /**
     * Get a copy of the producer properties for this Kafka client.
     * @return the producer properties; never null
     */
    Properties kafkaProducerProperties() {
        return new Properties(producerProperties);
    }
    
    Properties kafkaConsumerProperties() {
        return new Properties(consumerProperties);
    }
    
    Properties kafkaConsumerProperties( String groupId ) {
        Properties props = kafkaConsumerProperties();
        props.put("group.id",groupId);
        return props;
    }
}
